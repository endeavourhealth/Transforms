package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceComponents;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingOutpatientCdsTarget;
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

public class OutpatientCdsTargetTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(OutpatientCdsTargetTransformer.class);

    public static void transform(FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        try {
            createOutpatientCdsEncounters(fhirResourceFiler, csvHelper);
        } catch (Exception ex) {
            fhirResourceFiler.logTransformRecordError(ex, null);
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void createOutpatientCdsEncounters(FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        // retrieve the target outpatient cds records for the current exchangeId
        List<StagingOutpatientCdsTarget> targetOutpatientCdsRecords = csvHelper.retrieveTargetOutpatientCds();
        if (targetOutpatientCdsRecords == null) {
            return;
        }

        for (StagingOutpatientCdsTarget targetOutpatientCds : targetOutpatientCdsRecords) {

            boolean isDeleted = targetOutpatientCds.isDeleted();
            if (isDeleted) {

                deleteOutpatientCdsEncounterAndChildren(targetOutpatientCds, fhirResourceFiler, csvHelper);
                continue;
            }

            //process top level encounter - the existing parent encounter set during ADT feed -
            Integer encounterId = targetOutpatientCds.getEncounterId();  //this is used to identify the top level parent episode
            if (encounterId != null) {

                Encounter existingParentEncounter
                        = (Encounter) csvHelper.retrieveResourceForLocalId(ResourceType.Encounter, Integer.toString(encounterId));
                if (existingParentEncounter != null) {

                    //update the top level encounter
                    updateExistingEncounter(existingParentEncounter, targetOutpatientCds, fhirResourceFiler, csvHelper);

                    //create the linked child encounter
                    createOutpatientCdsSubEncounter(targetOutpatientCds, fhirResourceFiler, csvHelper);

                } else {

                    //create top level parent with minimum data
                    createOutpatientCdsEncounterParentAndSub(targetOutpatientCds, fhirResourceFiler, csvHelper);

                }
            } else {

                String uniqueId = targetOutpatientCds.getUniqueId();
                throw new Exception("encounter_id missing for Outpatient CDS record: "+uniqueId);
            }
        }
    }

    private static void deleteOutpatientCdsEncounterAndChildren(StagingOutpatientCdsTarget targetOutpatientCds,
                                                         FhirResourceFiler fhirResourceFiler,
                                                         BartsCsvHelper csvHelper) throws Exception {

        Integer encounterId = targetOutpatientCds.getEncounterId();  //this is used to identify the top level parent episode

        //retrieve the existing Top level parent Encounter resource to perform a deletion plus any child encounters
        Encounter existingParentEncounter
                = (Encounter) csvHelper.retrieveResourceForLocalId(ResourceType.Encounter, Integer.toString(encounterId));

        if (existingParentEncounter != null) {

            EncounterBuilder parentEncounterBuilder
                    = new EncounterBuilder(existingParentEncounter, targetOutpatientCds.getAudit());

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
                    }
                }
            }

            //finally, delete the top level parent
            fhirResourceFiler.deletePatientResource(null, false, parentEncounterBuilder);

        } else {
            TransformWarnings.log(LOG, csvHelper, "Cannot find existing Encounter: {} for deletion", encounterId);
        }
    }

    private static void createOutpatientCdsSubEncounter(StagingOutpatientCdsTarget targetOutpatientCds,
                                                     FhirResourceFiler fhirResourceFiler,
                                                     BartsCsvHelper csvHelper) throws Exception {

        //set outpatient encounter
        EncounterBuilder encounterBuilder = new EncounterBuilder();
        encounterBuilder.setClass(Encounter.EncounterClass.OUTPATIENT);
        encounterBuilder.setStatus(Encounter.EncounterState.FINISHED);

        CodeableConceptBuilder codeableConceptBuilder
                = new CodeableConceptBuilder(encounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
        codeableConceptBuilder.setText("Outpatient Attendance");

        //the unique Id for the outpatient encounter based on attendance identifier
        String attendanceId = targetOutpatientCds.getApptAttendanceIdentifier() + ":OP";
        encounterBuilder.setId(attendanceId);

        Date appDate = targetOutpatientCds.getApptDate();
        encounterBuilder.setPeriodStart(appDate);

        setCommonEncounterAttributes(encounterBuilder, targetOutpatientCds, csvHelper, true);

        //add in additional extended data as Parameters resource with additional extension
        //TODO: set name and values using IM map once done, i.e. replace referral_source etc.
        ContainedParametersBuilder containedParametersBuilder = new ContainedParametersBuilder(encounterBuilder);
        containedParametersBuilder.removeContainedParameters();

        String adminCategoryCode = targetOutpatientCds.getAdministrativeCategoryCode();
        if (!Strings.isNullOrEmpty(adminCategoryCode)) {
            containedParametersBuilder.addParameter("DM_hasAdministrativeCategoryCode", "CM_AdminCat" + adminCategoryCode);
        }
        String referralSourceId = targetOutpatientCds.getReferralSource();
        if (!Strings.isNullOrEmpty(referralSourceId)) {
            containedParametersBuilder.addParameter("referral_source", "" + referralSourceId);
        }
        String apptAttendedCode = targetOutpatientCds.getApptAttendedCode();
        if (!Strings.isNullOrEmpty(apptAttendedCode)) {
            containedParametersBuilder.addParameter("appt_attended_code", "" + apptAttendedCode);
        }
        String apptOutcomeCode = targetOutpatientCds.getApptAttendedCode();
        if (!Strings.isNullOrEmpty(apptOutcomeCode)) {
            containedParametersBuilder.addParameter("appt_outcome_code", "" + apptOutcomeCode);
        }
        //this is a Cerner code which is mapped to an NHS DD alias
        String treatmentFunctionCode = targetOutpatientCds.getTreatmentFunctionCode();
        if (!Strings.isNullOrEmpty(treatmentFunctionCode)) {
            CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.TREATMENT_FUNCTION, treatmentFunctionCode);
            if (codeRef != null) {

                String treatmentFunctionCodeNHSAliasCode = codeRef.getAliasNhsCdAlias();
                containedParametersBuilder.addParameter("treatment_function", "" + treatmentFunctionCodeNHSAliasCode);
            }
        }

        //add in diagnosis or procedure data match the encounter date? - already processed via proc and diag CDS transforms

//            if (!Strings.isNullOrEmpty(targetOutpatientCds.getPrimaryDiagnosisICD())) {
//                additionalArrivalObjs.addProperty("primary_diagnosis", targetOutpatientCds.getPrimaryDiagnosisICD());
//            }
//            if (!Strings.isNullOrEmpty(targetOutpatientCds.getSecondaryDiagnosisICD())) {
//                additionalArrivalObjs.addProperty("secondary_diagnosis", targetOutpatientCds.getSecondaryDiagnosisICD());
//            }
//            if (!Strings.isNullOrEmpty(targetOutpatientCds.getOtherDiagnosisICD())) {
//                additionalArrivalObjs.addProperty("other_diagnosis", targetOutpatientCds.getOtherDiagnosisICD());
//            }
//            if (!Strings.isNullOrEmpty(targetOutpatientCds.getPrimaryProcedureOPCS())) {
//                additionalArrivalObjs.addProperty("primary_procedure", targetOutpatientCds.getPrimaryProcedureOPCS());
//            }
//            if (!Strings.isNullOrEmpty(targetOutpatientCds.getSecondaryProcedureOPCS())) {
//                additionalArrivalObjs.addProperty("secondary_procedure", targetOutpatientCds.getSecondaryProcedureOPCS());
//            }
//            if (!Strings.isNullOrEmpty(targetOutpatientCds.getOtherProceduresOPCS())) {
//                additionalArrivalObjs.addProperty("other_procedures", targetOutpatientCds.getOtherProceduresOPCS());
//            }

        ///retrieve and update the parent to point to this new child encounter
        Integer parentEncounterId = targetOutpatientCds.getEncounterId();
        Encounter existingParentEncounter
                = (Encounter) csvHelper.retrieveResourceForLocalId(ResourceType.Encounter, Integer.toString(parentEncounterId));
        EncounterBuilder existingParentEncounterBuilder = new EncounterBuilder(existingParentEncounter);
        //and link the parent to this new child encounter
        Reference childCriticalRef = ReferenceHelper.createReference(ResourceType.Encounter, attendanceId);
        ContainedListBuilder listBuilder = new ContainedListBuilder(existingParentEncounterBuilder);
        listBuilder.addReference(childCriticalRef);

        //save encounterBuilder records
        fhirResourceFiler.savePatientResource(null, encounterBuilder, existingParentEncounterBuilder);
    }

    private static void createOutpatientCdsEncounterParentAndSub(StagingOutpatientCdsTarget targetOutpatientCds,
                                                                    FhirResourceFiler fhirResourceFiler,
                                                                    BartsCsvHelper csvHelper) throws Exception {

        EncounterBuilder parentTopEncounterBuilder = new EncounterBuilder();
        parentTopEncounterBuilder.setClass(Encounter.EncounterClass.OUTPATIENT);
        Integer encounterId = targetOutpatientCds.getEncounterId();
        parentTopEncounterBuilder.setId(Integer.toString(encounterId));

        parentTopEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);

        Date appDate = targetOutpatientCds.getApptDate();
        parentTopEncounterBuilder.setPeriodStart(appDate);

        CodeableConceptBuilder codeableConceptBuilder
                = new CodeableConceptBuilder(parentTopEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
        codeableConceptBuilder.setText("Outpatient");

        setCommonEncounterAttributes(parentTopEncounterBuilder, targetOutpatientCds, csvHelper, false);

        //save encounterBuilder record
        fhirResourceFiler.savePatientResource(null, parentTopEncounterBuilder);

        //wait until parent resources are filed
        csvHelper.waitUntilThreadPoolIsEmpty();

        //then create child level encounter linked to this new parent
        createOutpatientCdsSubEncounter(targetOutpatientCds, fhirResourceFiler, csvHelper);
    }

    private static void updateExistingEncounter(Encounter existingEncounter,
                                                StagingOutpatientCdsTarget targetOutpatientCds,
                                                FhirResourceFiler fhirResourceFiler,
                                                BartsCsvHelper csvHelper) throws Exception {

        EncounterBuilder existingEncounterBuilder
                = new EncounterBuilder(existingEncounter, targetOutpatientCds.getAudit());

        //todo - deceide on how much to update the top level with

        String cdsUniqueId = targetOutpatientCds.getUniqueId();
        if (!cdsUniqueId.isEmpty()) {

            cdsUniqueId = cdsUniqueId.replaceFirst("OPCDS-","");
            IdentifierBuilder.removeExistingIdentifiersForSystem(existingEncounterBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CDS_UNIQUE_ID);
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(existingEncounterBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CDS_UNIQUE_ID);
            identifierBuilder.setValue(cdsUniqueId);
        }

        fhirResourceFiler.savePatientResource(null, existingEncounterBuilder);
    }

    private static void setCommonEncounterAttributes(EncounterBuilder builder,
                                                     StagingOutpatientCdsTarget targetOutpatientCds,
                                                     BartsCsvHelper csvHelper,
                                                     boolean isChildEncounter) throws Exception  {

        //every encounter has the following common attributes
        Integer personId = targetOutpatientCds.getPersonId();
        if (personId != null) {
            Reference patientReference
                    = ReferenceHelper.createReference(ResourceType.Patient, personId.toString());
            if (builder.isIdMapped()) {

                patientReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, csvHelper);
            }
            builder.setPatient(patientReference);
        }
        Integer episodeId = targetOutpatientCds.getEpisodeId();
        if (episodeId != null) {

            Reference episodeReference
                    = ReferenceHelper.createReference(ResourceType.EpisodeOfCare, episodeId.toString());
            if (builder.isIdMapped()) {

                episodeReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(episodeReference, csvHelper);
            }
            builder.setEpisodeOfCare(episodeReference);
        }
        Integer performerPersonnelId = targetOutpatientCds.getPerformerPersonnelId();
        if (performerPersonnelId != null) {

            Reference practitionerReference
                    = ReferenceHelper.createReference(ResourceType.Practitioner, Integer.toString(performerPersonnelId));
            if (builder.isIdMapped()) {

                practitionerReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, csvHelper);
            }
            builder.addParticipant(practitionerReference, EncounterParticipantType.PRIMARY_PERFORMER);
        }
        String serviceProviderOrgId = targetOutpatientCds.getApptSiteCode();
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
            Integer parentEncounterId = targetOutpatientCds.getEncounterId();
            Reference parentEncounter
                    = ReferenceHelper.createReference(ResourceType.Encounter, Integer.toString(parentEncounterId));
            if (builder.isIdMapped()) {

                parentEncounter = IdHelper.convertLocallyUniqueReferenceToEdsReference(parentEncounter, csvHelper);
            }
            builder.setPartOf(parentEncounter);
        }
        //set the CDS identifier against the Encounter
        String cdsUniqueId = targetOutpatientCds.getUniqueId();
        if (!cdsUniqueId.isEmpty()) {

            cdsUniqueId = cdsUniqueId.replaceFirst("OPCDS-","");
            IdentifierBuilder.removeExistingIdentifiersForSystem(builder, FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CDS_UNIQUE_ID);
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(builder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CDS_UNIQUE_ID);
            identifierBuilder.setValue(cdsUniqueId);
        }
        String pathwayIdentifier = targetOutpatientCds.getPatientPathwayIdentifier();
        if (!pathwayIdentifier.isEmpty()) {

            IdentifierBuilder.removeExistingIdentifiersForSystem(builder, FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_PATHWAY_ID);
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(builder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_PATHWAY_ID);
            identifierBuilder.setValue(pathwayIdentifier);
        }
    }
}