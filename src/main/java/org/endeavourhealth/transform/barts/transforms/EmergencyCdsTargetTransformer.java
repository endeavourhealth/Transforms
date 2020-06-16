package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.apache.commons.lang3.ObjectUtils;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceComponents;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingEmergencyCdsTarget;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class EmergencyCdsTargetTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(EmergencyCdsTargetTransformer.class);

    public static void transform(FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        try {
            createEmergencyCdsEncounters(fhirResourceFiler, csvHelper);
        } catch (Exception ex) {
            fhirResourceFiler.logTransformRecordError(ex, null);
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }


    public static void createEmergencyCdsEncounters(FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        // retrieve the target emergency cds records for the current exchangeId
        List<StagingEmergencyCdsTarget> targetEmergencyCdsRecords = csvHelper.retrieveTargetEmergencyCds();
        if (targetEmergencyCdsRecords == null) {
            return;
        }

        for (StagingEmergencyCdsTarget targetEmergencyCds : targetEmergencyCdsRecords) {

            boolean isDeleted = targetEmergencyCds.isDeleted();
            if (isDeleted) {

                deleteEmergencyCdsEncounterAndChildren(targetEmergencyCds, fhirResourceFiler, csvHelper);
                continue;
            }

            //process top level encounter - the existing parent encounter set during ADT feed -
            Integer encounterId = targetEmergencyCds.getEncounterId();  //this is used to identify the top level parent episode

            if (encounterId != null) {

                Encounter existingParentEncounter
                        = (Encounter) csvHelper.retrieveResourceForLocalId(ResourceType.Encounter, Integer.toString(encounterId));
                if (existingParentEncounter != null) {

                    //update the existing top level encounter
                    updateExistingParentEncounter(existingParentEncounter, targetEmergencyCds, fhirResourceFiler, csvHelper);

                    //create the linked child encounters
                    EncounterBuilder parentEncounterBuilder = new EncounterBuilder(existingParentEncounter);
                    createEmergencyCdsEncounters(targetEmergencyCds, fhirResourceFiler, csvHelper, parentEncounterBuilder);

                } else {

                    //create top level parent with minimum data and the sub encounters
                    createEmergencyCdsEncounterParentAndSubs(targetEmergencyCds, fhirResourceFiler, csvHelper);
                }
            } else {

                String uniqueId = targetEmergencyCds.getUniqueId();
                throw new Exception("encounter_id missing for Inpatient CDS record: " + uniqueId);
            }
        }
    }

    private static void createEmergencyCdsEncounters(StagingEmergencyCdsTarget targetEmergencyCds,
                                                     FhirResourceFiler fhirResourceFiler,
                                                     BartsCsvHelper csvHelper,
                                                     EncounterBuilder existingParentEpisodeBuilder) throws Exception {


        ///retrieve the parent encounter (if not passed in) to point to any new child encounters created during this method
        if (existingParentEpisodeBuilder == null) {
            Integer parentEncounterId = targetEmergencyCds.getEncounterId();
            Encounter existingParentEncounter
                    = (Encounter) csvHelper.retrieveResourceForLocalId(ResourceType.Encounter, Integer.toString(parentEncounterId));
            existingParentEpisodeBuilder = new EncounterBuilder(existingParentEncounter);
        }
        ContainedListBuilder existingEncounterList = new ContainedListBuilder(existingParentEpisodeBuilder);

        //unique to the emergency dept. attendance
        String attendanceId = targetEmergencyCds.getAttendanceId();

        ////start with the A&E arrival encounter///////////////////////////////////////////////////////////////////////
        EncounterBuilder arrivalEncounterBuilder = new EncounterBuilder();
        arrivalEncounterBuilder.setClass(Encounter.EncounterClass.EMERGENCY);

        String arrivalEncounterId = attendanceId + ":01:EM";
        arrivalEncounterBuilder.setId(arrivalEncounterId);
        Date arrivalDate = targetEmergencyCds.getDtArrival();
        arrivalEncounterBuilder.setPeriodStart(arrivalDate);
        arrivalEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);

        CodeableConceptBuilder codeableConceptBuilderAdmission
                = new CodeableConceptBuilder(arrivalEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
        codeableConceptBuilderAdmission.setText("Emergency Arrival");

        setCommonEncounterAttributes(arrivalEncounterBuilder, targetEmergencyCds, csvHelper, true);

        //and link the parent to this new child encounter
        Reference childArrivalRef = ReferenceHelper.createReference(ResourceType.Encounter, arrivalEncounterId);
        existingEncounterList.addReference(childArrivalRef);

        //add in additional extended data as Parameters resource with additional extension
        //TODO: set name and values using IM map once done, i.e. replace ae_arrival_mode etc.
        ContainedParametersBuilder containedParametersBuilderArrival
                = new ContainedParametersBuilder(arrivalEncounterBuilder);
        containedParametersBuilderArrival.removeContainedParameters();

        String aeAttendanceCategoryCode = targetEmergencyCds.getAttendanceCategory();
        if (!Strings.isNullOrEmpty(aeAttendanceCategoryCode)) {
            containedParametersBuilderArrival.addParameter("ae_attendance_category", "" + aeAttendanceCategoryCode);
        }
        String aeAttendanceSource = targetEmergencyCds.getAttendanceSource();
        if (!Strings.isNullOrEmpty(aeAttendanceSource)) {
            containedParametersBuilderArrival.addParameter("ae_attendance_source", "" + aeAttendanceSource);
        }
        String aeDepartmentType = targetEmergencyCds.getDepartmentType();
        if (!Strings.isNullOrEmpty(aeDepartmentType)) {
            containedParametersBuilderArrival.addParameter("ae_department_type", "" + aeDepartmentType);
        }
        String aeArrivalMode = targetEmergencyCds.getArrivalMode();
        if (!Strings.isNullOrEmpty(aeArrivalMode)) {
            containedParametersBuilderArrival.addParameter("ae_arrival_mode", "" + aeArrivalMode);
        }
        String chiefComplaint = targetEmergencyCds.getChiefComplaint();
        if (!Strings.isNullOrEmpty(chiefComplaint)) {
            containedParametersBuilderArrival.addParameter("ae_chief_complaint", "" + chiefComplaint);
        }
        //this is a Cerner code which is mapped to an NHS DD alias
        String treatmentFunctionCode = targetEmergencyCds.getTreatmentFunctionCode();
        if (!Strings.isNullOrEmpty(treatmentFunctionCode)) {
            CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.TREATMENT_FUNCTION, treatmentFunctionCode);
            if (codeRef != null) {

                String treatmentFunctionCodeNHSAliasCode = codeRef.getAliasNhsCdAlias();
                containedParametersBuilderArrival.addParameter("treatment_function", "" + treatmentFunctionCodeNHSAliasCode);
            }
        }

        //check for other dates to determine if the arrival has ended
        Date assessmentDate = targetEmergencyCds.getDtInitialAssessment();
        Date invAndTreatmentsDate = targetEmergencyCds.getDtSeenForTreatment();
        Date admitDate = targetEmergencyCds.getDtDecidedToAdmit();
        Date conclusionDate = targetEmergencyCds.getDtConclusion();
        Date dischargeDate = targetEmergencyCds.getDtDeparture();
        Date aeEndDate
                = ObjectUtils.firstNonNull(assessmentDate, invAndTreatmentsDate, admitDate, conclusionDate, dischargeDate);
        if (aeEndDate != null) {

            arrivalEncounterBuilder.setPeriodEnd(aeEndDate);
            arrivalEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
        }

        ////Is there an initial assessment encounter?///////////////////////////////////////////////////////////////////
        EncounterBuilder assessmentEncounterBuilder = null;
        if (assessmentDate != null) {

            assessmentEncounterBuilder = new EncounterBuilder();
            assessmentEncounterBuilder.setClass(Encounter.EncounterClass.EMERGENCY);

            String assessmentEncounterId = attendanceId + ":02:EM";
            assessmentEncounterBuilder.setId(assessmentEncounterId);
            assessmentEncounterBuilder.setPeriodStart(assessmentDate);
            assessmentEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);

            CodeableConceptBuilder codeableConceptBuilderAssessment
                    = new CodeableConceptBuilder(assessmentEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
            codeableConceptBuilderAssessment.setText("Emergency Initial Assessment");

            setCommonEncounterAttributes(assessmentEncounterBuilder, targetEmergencyCds, csvHelper, true);

            //and link the parent to this new child encounter
            Reference childAssessmentRef = ReferenceHelper.createReference(ResourceType.Encounter, assessmentEncounterId);
            existingEncounterList.addReference(childAssessmentRef);

            //add in additional extended data as Parameters resource with additional extension
            //TODO: set name and values using IM map once done, i.e. replace ae_arrival_mode etc.
            ContainedParametersBuilder containedParametersBuilderAss
                    = new ContainedParametersBuilder(assessmentEncounterBuilder);
            containedParametersBuilderAss.removeContainedParameters();

            String safeGuardingConcerns = targetEmergencyCds.getSafeguardingConcerns();
            if (!Strings.isNullOrEmpty(safeGuardingConcerns)) {
                containedParametersBuilderAss.addParameter("safe_guarding_concerns", "" + safeGuardingConcerns);
            }

            Date aeAssessmentEndDate
                    = ObjectUtils.firstNonNull(invAndTreatmentsDate, admitDate, conclusionDate, dischargeDate);
            if (aeAssessmentEndDate != null) {

                assessmentEncounterBuilder.setPeriodEnd(aeAssessmentEndDate);
                assessmentEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
            }
        }

        ////Is there a treatments encounter?////////////////////////////////////////////////////////////////////////////
        EncounterBuilder treatmentsEncounterBuilder = null;
        if (invAndTreatmentsDate != null) {

            treatmentsEncounterBuilder = new EncounterBuilder();
            treatmentsEncounterBuilder.setClass(Encounter.EncounterClass.EMERGENCY);

            String treatmentsEncounterId = attendanceId + ":03:EM";
            treatmentsEncounterBuilder.setId(treatmentsEncounterId);
            treatmentsEncounterBuilder.setPeriodStart(invAndTreatmentsDate);
            treatmentsEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);

            CodeableConceptBuilder codeableConceptBuilderTreatments
                    = new CodeableConceptBuilder(treatmentsEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
            codeableConceptBuilderTreatments.setText("Emergency Investigations and Treatments");

            setCommonEncounterAttributes(treatmentsEncounterBuilder, targetEmergencyCds, csvHelper, true);

            //and link the parent to this new child encounter
            Reference childTreatmentsRef = ReferenceHelper.createReference(ResourceType.Encounter, treatmentsEncounterId);
            existingEncounterList.addReference(childTreatmentsRef);

            //TODO - do we save the linked clinical data here?
            //targetEmergencyCds.getDiagnosis();
            //targetEmergencyCds.getInvestigations();
            //targetEmergencyCds.getTreatments();

            String referredToServices = targetEmergencyCds.getReferredToServices();
            if (!Strings.isNullOrEmpty(referredToServices)) {

                //TODO:  create referrals(s) linked to main encounter_id or ParametersList?
            }

            Date aeTreatmentsEndDate
                    = ObjectUtils.firstNonNull(admitDate, conclusionDate, dischargeDate);
            if (aeTreatmentsEndDate != null) {

                treatmentsEncounterBuilder.setPeriodEnd(aeTreatmentsEndDate);
                treatmentsEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
            }
        }


        ////Is there a discharge encounter?/////////////////////////////////////////////////////////////////////////////
        EncounterBuilder dischargeEncounterBuilder = null;
        if (dischargeDate != null) {

            dischargeEncounterBuilder = new EncounterBuilder();
            dischargeEncounterBuilder.setClass(Encounter.EncounterClass.EMERGENCY);

            String dischargeEncounterId = attendanceId + ":04:EM";
            dischargeEncounterBuilder.setId(dischargeEncounterId);
            dischargeEncounterBuilder.setPeriodStart(dischargeDate);
            dischargeEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);

            CodeableConceptBuilder codeableConceptBuilderDischarge
                    = new CodeableConceptBuilder(dischargeEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
            codeableConceptBuilderDischarge.setText("Emergency Discharge");

            setCommonEncounterAttributes(dischargeEncounterBuilder, targetEmergencyCds, csvHelper, true);

            //and link the parent to this new child encounter
            Reference childDischargeRef = ReferenceHelper.createReference(ResourceType.Encounter, dischargeEncounterId);
            existingEncounterList.addReference(childDischargeRef);

            //add in additional extended data as Parameters resource with additional extension
            //TODO: set name and values using IM map once done, i.e. replace ae_arrival_mode etc.
            ContainedParametersBuilder containedParametersBuilderDischarge
                    = new ContainedParametersBuilder(dischargeEncounterBuilder);
            containedParametersBuilderDischarge.removeContainedParameters();

            String dischargeStatusCode = targetEmergencyCds.getDischargeStatus();
            if (!Strings.isNullOrEmpty(dischargeStatusCode)) {
                containedParametersBuilderDischarge.addParameter("ae_discharge_status", "" + dischargeStatusCode);
            }
            String dischargeDestinationCode = targetEmergencyCds.getDischargeDestination();
            if (!Strings.isNullOrEmpty(dischargeDestinationCode)) {
                containedParametersBuilderDischarge.addParameter("ae_discharge_destination", "" + dischargeDestinationCode);
            }

            Date aeDischargeEndDate
                    = ObjectUtils.firstNonNull(conclusionDate, dischargeDate);
            if (aeDischargeEndDate != null) {

                dischargeEncounterBuilder.setPeriodEnd(aeDischargeEndDate);
                dischargeEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
            }
        }

        //save the existing parent encounter here with the updated child refs added during this method,
        //then the child sub encounter afterwards
        fhirResourceFiler.savePatientResource(null, existingParentEpisodeBuilder);

        //save the A&E arrival encounter
        if (arrivalEncounterBuilder != null) {
            fhirResourceFiler.savePatientResource(null, arrivalEncounterBuilder);
        }
        //save the A&E assessment encounter
        if (assessmentEncounterBuilder != null) {
            fhirResourceFiler.savePatientResource(null, assessmentEncounterBuilder);
        }
        //save the A&E treatments encounter
        if (treatmentsEncounterBuilder != null) {
            fhirResourceFiler.savePatientResource(null, treatmentsEncounterBuilder);
        }
        //save the A&E discharge encounter
        if (dischargeEncounterBuilder != null) {
            fhirResourceFiler.savePatientResource(null, dischargeEncounterBuilder);
        }
    }

    private static void createEmergencyCdsEncounterParentAndSubs(StagingEmergencyCdsTarget targetEmergencyCds,
                                                                 FhirResourceFiler fhirResourceFiler,
                                                                 BartsCsvHelper csvHelper) throws Exception {

        EncounterBuilder parentTopEncounterBuilder = new EncounterBuilder();
        parentTopEncounterBuilder.setClass(Encounter.EncounterClass.EMERGENCY);

        Integer encounterId = targetEmergencyCds.getEncounterId();
        parentTopEncounterBuilder.setId(Integer.toString(encounterId));

        Date arrivalDate = targetEmergencyCds.getDtArrival();
        parentTopEncounterBuilder.setPeriodStart(arrivalDate);

        Date departureDate = targetEmergencyCds.getDtDeparture();
        if (departureDate != null) {

            parentTopEncounterBuilder.setPeriodEnd(departureDate);
            parentTopEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
        } else {

            parentTopEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);
        }

        CodeableConceptBuilder codeableConceptBuilder
                = new CodeableConceptBuilder(parentTopEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
        codeableConceptBuilder.setText("Emergency");

        setCommonEncounterAttributes(parentTopEncounterBuilder, targetEmergencyCds, csvHelper, false);

        //then create child level encounters linked to this new parent
        createEmergencyCdsEncounters(targetEmergencyCds, fhirResourceFiler, csvHelper, parentTopEncounterBuilder);
    }

    private static void setCommonEncounterAttributes(EncounterBuilder builder,
                                                     StagingEmergencyCdsTarget targetEmergencyCds,
                                                     BartsCsvHelper csvHelper,
                                                     boolean isChildEncounter) throws Exception  {

        //every encounter has the following common attributes
        Integer personId = targetEmergencyCds.getPersonId();
        if (personId !=null) {
            Reference patientReference
                    = ReferenceHelper.createReference(ResourceType.Patient, personId.toString());
            if (builder.isIdMapped()) {

                patientReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, csvHelper);
            }
            builder.setPatient(patientReference);
        }
        Integer episodeId = targetEmergencyCds.getEpisodeId();
        if (episodeId != null) {

            Reference episodeReference
                    = ReferenceHelper.createReference(ResourceType.EpisodeOfCare, episodeId.toString());
            if (builder.isIdMapped()) {

                episodeReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(episodeReference, csvHelper);
            }
            builder.setEpisodeOfCare(episodeReference);
        }
        Integer performerPersonnelId = targetEmergencyCds.getPerformerPersonnelId();
        if (performerPersonnelId != null) {

            Reference practitionerReference
                    = ReferenceHelper.createReference(ResourceType.Practitioner, Integer.toString(performerPersonnelId));
            if (builder.isIdMapped()) {

                practitionerReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, csvHelper);
            }
            builder.addParticipant(practitionerReference, EncounterParticipantType.PRIMARY_PERFORMER);
        }
        String serviceProviderOrgId = targetEmergencyCds.getOrganisationCode();
        if (!Strings.isNullOrEmpty(serviceProviderOrgId)) {

            Reference organizationReference
                    = ReferenceHelper.createReference(ResourceType.Organization, serviceProviderOrgId);
            if (builder.isIdMapped()) {

                organizationReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(organizationReference, csvHelper);
            }
            builder.setServiceProvider(organizationReference);
        }
        //get the existing parent encounter set during ADT feed, to link to this top level encounter if this is a child
        if (isChildEncounter) {
            Integer encounterId = targetEmergencyCds.getEncounterId();
            Reference parentEncounter
                    = ReferenceHelper.createReference(ResourceType.Encounter, Integer.toString(encounterId));
            //if (builder.isIdMapped()) {

                parentEncounter = IdHelper.convertLocallyUniqueReferenceToEdsReference(parentEncounter, csvHelper);
            //}
            builder.setPartOf(parentEncounter);
        }
        //set the CDS identifier against the Encounter
        String cdsUniqueId = targetEmergencyCds.getUniqueId();
        if (!cdsUniqueId.isEmpty()) {

            cdsUniqueId = cdsUniqueId.replaceFirst("ECDS-","");
            IdentifierBuilder.removeExistingIdentifiersForSystem(builder, FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CDS_UNIQUE_ID);
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(builder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CDS_UNIQUE_ID);
            identifierBuilder.setValue(cdsUniqueId);
        }
    }

    private static void deleteEmergencyCdsEncounterAndChildren(StagingEmergencyCdsTarget targetEmergencyCds,
                                                               FhirResourceFiler fhirResourceFiler,
                                                               BartsCsvHelper csvHelper) throws Exception {

        Integer encounterId = targetEmergencyCds.getEncounterId();  //this is used to identify the top level parent episode

        //retrieve the existing Top level parent Encounter resource to perform a deletion plus any child encounters
        Encounter existingParentEncounter
                = (Encounter) csvHelper.retrieveResourceForLocalId(ResourceType.Encounter, Integer.toString(encounterId));

        if (existingParentEncounter != null) {

            EncounterBuilder parentEncounterBuilder
                    = new EncounterBuilder(existingParentEncounter, targetEmergencyCds.getAudit());

            //has this encounter got child encounters?
            if (existingParentEncounter.hasContained()) {

                ContainedListBuilder listBuilder = new ContainedListBuilder(parentEncounterBuilder);
                ResourceDalI resourceDal = DalProvider.factoryResourceDal();

                for (List_.ListEntryComponent item: listBuilder.getContainedListItems()) {
                    Reference ref = item.getItem();
                    ReferenceComponents comps = ReferenceHelper.getReferenceComponents(ref);
                    if (comps.getResourceType() != ResourceType.Encounter) {
                        continue;
                    }
                    Encounter childEncounter
                            = (Encounter)resourceDal.getCurrentVersionAsResource(csvHelper.getServiceId(), ResourceType.Encounter, comps.getId());
                    if (childEncounter != null) {
                        LOG.debug("Deleting child encounter " + childEncounter.getId());

                        fhirResourceFiler.deletePatientResource(null, false, new EncounterBuilder(childEncounter));
                    } else {

                        TransformWarnings.log(LOG, csvHelper, "Cannot find existing child Encounter: {} for deletion", childEncounter.getId());
                    }
                }
            }

            //finally, delete the top level parent
            fhirResourceFiler.deletePatientResource(null, false, parentEncounterBuilder);

        } else {
            TransformWarnings.log(LOG, csvHelper, "Cannot find existing Encounter: {} for deletion", encounterId);
        }
    }

    private static void updateExistingParentEncounter(Encounter existingEncounter,
                                                      StagingEmergencyCdsTarget targetEmergencyCds,
                                                      FhirResourceFiler fhirResourceFiler,
                                                      BartsCsvHelper csvHelper) throws Exception {

        EncounterBuilder existingEncounterBuilder
                = new EncounterBuilder(existingEncounter, targetEmergencyCds.getAudit());

        //todo - decide on how much to update the top level with
        Date departureDate = targetEmergencyCds.getDtDeparture();
        if (departureDate != null) {

            existingEncounterBuilder.setPeriodEnd(departureDate);
            existingEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
        } else {

            //may not have been discharged, i.e. passed away
            Date conclusionDate = targetEmergencyCds.getDtConclusion();
            if (conclusionDate != null) {

                existingEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);

            } else {

                existingEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);
            }
        }

        String cdsUniqueId = targetEmergencyCds.getUniqueId();
        if (!cdsUniqueId.isEmpty()) {

            cdsUniqueId = cdsUniqueId.replaceFirst("ECDS-","");
            IdentifierBuilder.removeExistingIdentifiersForSystem(existingEncounterBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CDS_UNIQUE_ID);
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(existingEncounterBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CDS_UNIQUE_ID);
            identifierBuilder.setValue(cdsUniqueId);
        }

        fhirResourceFiler.savePatientResource(null, existingEncounterBuilder);
    }
}