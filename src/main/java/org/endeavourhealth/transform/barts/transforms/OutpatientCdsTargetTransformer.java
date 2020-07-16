package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceComponents;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingOutpatientCdsTarget;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.im.client.IMClient;
import org.endeavourhealth.im.models.mapping.MapColumnRequest;
import org.endeavourhealth.im.models.mapping.MapColumnValueRequest;
import org.endeavourhealth.im.models.mapping.MapResponse;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.UUID;

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

            //check if any patient filtering applied
            String personId = Integer.toString(targetOutpatientCds.getPersonId());
            if (!csvHelper.processRecordFilteringOnPatientId(personId)) {
                continue;
            }

            boolean isDeleted = targetOutpatientCds.isDeleted();
            if (isDeleted) {

                deleteOutpatientCdsEncounterAndChildren(targetOutpatientCds, fhirResourceFiler, csvHelper);
                continue;
            }

            //process top level encounter - the existing parent encounter
            Integer encounterId = targetOutpatientCds.getEncounterId();  //this is used to identify the top level parent episode
            if (encounterId != null) {

                EncounterBuilder parentEncounterBuilder = null;

                Encounter existingParentEncounter
                        = (Encounter) csvHelper.retrieveResourceForLocalId(ResourceType.Encounter, Integer.toString(encounterId));
                if (existingParentEncounter != null) {

                    //update the top level encounter
                    updateExistingEncounter(existingParentEncounter, targetOutpatientCds, fhirResourceFiler, csvHelper);

                    //create the linked child encounter
                    parentEncounterBuilder = new EncounterBuilder(existingParentEncounter, targetOutpatientCds.getAudit());
                    createOutpatientCdsSubEncounter(targetOutpatientCds, fhirResourceFiler, csvHelper, parentEncounterBuilder);

                } else {

                    //create top level parent with minimum data and then the sub encounter
                    parentEncounterBuilder
                            = createOutpatientCdsEncounterParentAndSub(targetOutpatientCds, fhirResourceFiler, csvHelper);
                }

                //now delete any older HL7 Encounters for patients we've updated
                //but waiting until everything has been saved to the DB first
                fhirResourceFiler.waitUntilEverythingIsSaved();

                //find the patient UUID for the encounters we have just filed, so we can tidy up the
                //HL7 encounters after doing all the saving of the DW encounters
                Reference patientReference = parentEncounterBuilder.getPatient();
                if (!parentEncounterBuilder.isIdMapped()) {
                    patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
                }
                String patientUuid = ReferenceHelper.getReferenceId(patientReference);
                deleteHL7ReceiverPatientOutpatientEncounters(patientUuid, fhirResourceFiler, csvHelper);

            } else {

                String uniqueId = targetOutpatientCds.getUniqueId();
                LOG.warn("encounter_id missing for Outpatient CDS record: " + uniqueId);
                //throw new Exception("encounter_id missing for Outpatient CDS record: "+uniqueId);
            }
        }
    }

    private static void deleteOutpatientCdsEncounterAndChildren(StagingOutpatientCdsTarget targetOutpatientCds,
                                                         FhirResourceFiler fhirResourceFiler,
                                                         BartsCsvHelper csvHelper) throws Exception {

        Integer encounterId = targetOutpatientCds.getEncounterId();  //this is used to identify the top level parent episode

        //retrieve the existing Top level parent Encounter resource to perform a deletion plus any child encounters
        if (encounterId != null) {

            Encounter existingParentEncounter
                    = (Encounter) csvHelper.retrieveResourceForLocalId(ResourceType.Encounter, Integer.toString(encounterId));

            if (existingParentEncounter != null) {

                EncounterBuilder parentEncounterBuilder
                        = new EncounterBuilder(existingParentEncounter, targetOutpatientCds.getAudit());

                //has this encounter got child encounters?
                if (existingParentEncounter.hasContained()) {

                    ContainedListBuilder listBuilder = new ContainedListBuilder(parentEncounterBuilder);
                    ResourceDalI resourceDal = DalProvider.factoryResourceDal();

                    for (List_.ListEntryComponent item : listBuilder.getContainedListItems()) {
                        Reference ref = item.getItem();
                        ReferenceComponents comps = ReferenceHelper.getReferenceComponents(ref);
                        if (comps.getResourceType() != ResourceType.Encounter) {
                            continue;
                        }
                        Encounter childEncounter
                                = (Encounter) resourceDal.getCurrentVersionAsResource(csvHelper.getServiceId(), ResourceType.Encounter, comps.getId());
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
    }

    private static void createOutpatientCdsSubEncounter(StagingOutpatientCdsTarget targetOutpatientCds,
                                                        FhirResourceFiler fhirResourceFiler,
                                                        BartsCsvHelper csvHelper,
                                                        EncounterBuilder existingParentEncounterBuilder) throws Exception {

        //set outpatient encounter
        EncounterBuilder encounterBuilder = new EncounterBuilder();
        encounterBuilder.setClass(Encounter.EncounterClass.OUTPATIENT);
        encounterBuilder.setStatus(Encounter.EncounterState.FINISHED);  //sub encounters are always finished

        CodeableConceptBuilder codeableConceptBuilder
                = new CodeableConceptBuilder(encounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
        codeableConceptBuilder.setText("Outpatient Attendance");

        //the unique Id for the outpatient encounter based on attendance identifier
        String attendanceId = targetOutpatientCds.getApptAttendanceIdentifier() + ":OP";
        encounterBuilder.setId(attendanceId);

        Date appDate = targetOutpatientCds.getApptDate();
        encounterBuilder.setPeriodStart(appDate);

        setCommonEncounterAttributes(encounterBuilder, targetOutpatientCds, csvHelper, true, fhirResourceFiler);

        //add in additional extended data as Parameters resource with additional extension
        setAttendanceContainedParameters(encounterBuilder, targetOutpatientCds);

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

        ///retrieve (if not passed in) and update the parent to point to this new child encounter
        if (existingParentEncounterBuilder == null) {
            Integer parentEncounterId = targetOutpatientCds.getEncounterId();
            Encounter existingParentEncounter
                    = (Encounter) csvHelper.retrieveResourceForLocalId(ResourceType.Encounter, Integer.toString(parentEncounterId));
            existingParentEncounterBuilder = new EncounterBuilder(existingParentEncounter);
        }

        //and link the parent to this new child encounter

        Reference childOutpatientRef = ReferenceHelper.createReference(ResourceType.Encounter, attendanceId);
        ContainedListBuilder listBuilder = new ContainedListBuilder(existingParentEncounterBuilder);
        if (existingParentEncounterBuilder.isIdMapped()) {

            childOutpatientRef
                    = IdHelper.convertLocallyUniqueReferenceToEdsReference(childOutpatientRef, csvHelper);
        }
        listBuilder.addReference(childOutpatientRef);

        //save encounterBuilder records
        //LOG.debug("Saving existing OP parent encounter: "+FhirSerializationHelper.serializeResource(existingParentEncounterBuilder.getResource()));
        fhirResourceFiler.savePatientResource(null, !existingParentEncounterBuilder.isIdMapped(), existingParentEncounterBuilder);

        //LOG.debug("Saving child OP encounter: "+FhirSerializationHelper.serializeResource(encounterBuilder.getResource()));
        fhirResourceFiler.savePatientResource(null, encounterBuilder);
    }

    private static EncounterBuilder createOutpatientCdsEncounterParentAndSub(StagingOutpatientCdsTarget targetOutpatientCds,
                                                                    FhirResourceFiler fhirResourceFiler,
                                                                    BartsCsvHelper csvHelper) throws Exception {

        EncounterBuilder parentEncounterBuilder = new EncounterBuilder();
        parentEncounterBuilder.setClass(Encounter.EncounterClass.OUTPATIENT);
        Integer encounterId = targetOutpatientCds.getEncounterId();
        parentEncounterBuilder.setId(Integer.toString(encounterId));

        //if the outcome is discharged, then no more appointments for this parent encounter, so finished
        String apptOutcomeCode = targetOutpatientCds.getApptOutcomeCode();
        if (apptOutcomeCode.equalsIgnoreCase("1")) {
            parentEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
        } else {
            parentEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);
        }

        Date appDate = targetOutpatientCds.getApptDate();
        parentEncounterBuilder.setPeriodStart(appDate);

        CodeableConceptBuilder codeableConceptBuilder
                = new CodeableConceptBuilder(parentEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
        codeableConceptBuilder.setText("Outpatient");

        setCommonEncounterAttributes(parentEncounterBuilder, targetOutpatientCds, csvHelper, false, fhirResourceFiler);

        //then create child level encounter linked to this new parent
        createOutpatientCdsSubEncounter(targetOutpatientCds, fhirResourceFiler, csvHelper, parentEncounterBuilder);

        return parentEncounterBuilder;
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

        fhirResourceFiler.savePatientResource(null, !existingEncounterBuilder.isIdMapped(), existingEncounterBuilder);
    }

    private static void setCommonEncounterAttributes(EncounterBuilder builder,
                                                     StagingOutpatientCdsTarget targetOutpatientCds,
                                                     BartsCsvHelper csvHelper,
                                                     boolean isChildEncounter,
                                                     FhirResourceFiler fhirResourceFiler) throws Exception  {

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

        // Retrieve or create EpisodeOfCare for top level parent encounter only
        if (!isChildEncounter) {

            EpisodeOfCareBuilder episodeOfCareBuilder = csvHelper.getEpisodeOfCareCache().getEpisodeOfCareBuilder(targetOutpatientCds);
            if (episodeOfCareBuilder != null) {

                csvHelper.setEpisodeReferenceOnEncounter(episodeOfCareBuilder, builder, fhirResourceFiler);

                Date appDate = targetOutpatientCds.getApptDate();
                //we may have missed the original referral, so our episode of care may have the wrong start date, so adjust that now
                if (appDate != null) {
                    if (episodeOfCareBuilder.getRegistrationStartDate() == null
                            || appDate.before(episodeOfCareBuilder.getRegistrationStartDate())) {

                        episodeOfCareBuilder.setRegistrationStartDate(appDate);
                        episodeOfCareBuilder.setStatus(EpisodeOfCare.EpisodeOfCareStatus.ACTIVE);
                    }
                }

                // Check whether to Finish EpisodeOfCare by using the AppOutcomeCode
                // outcome corresponds to NHS Data Dictionary: https://www.datadictionary.nhs.uk/data_dictionary/attributes/o/out/outcome_of_attendance_de.asp?shownav=1
                // outcome = 1 means discharged from care, so no more appointments, so end of care
                String apptOutcomeCode = targetOutpatientCds.getApptOutcomeCode();
                if (apptOutcomeCode.equalsIgnoreCase("1")) {
                    // Discharged from CONSULTANT's care (last attendance)
                    // make sure to set the status AFTER setting the end date, as setting the end date
                    // will auto-calculate the status and we want to just overwrite that because we KNOW the episode is ended
                    episodeOfCareBuilder.setRegistrationEndDate(appDate);
                    episodeOfCareBuilder.setStatus(EpisodeOfCare.EpisodeOfCareStatus.FINISHED);
                }
            }
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
        //get the existing parent encounter to link to this top level encounter if this is a child
        if (isChildEncounter) {
            Integer parentEncounterId = targetOutpatientCds.getEncounterId();
            Reference parentEncounter
                    = ReferenceHelper.createReference(ResourceType.Encounter, Integer.toString(parentEncounterId));
            parentEncounter = IdHelper.convertLocallyUniqueReferenceToEdsReference(parentEncounter, csvHelper);
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

    /**
     * we match to some HL7 Receiver Encounters, basically taking them over
     * so we call this to tidy up (delete) any Episodes left not taken over, as the HL7 Receiver creates too many
     * episodes because it doesn't have the data to avoid doing so
     */
    private static void deleteHL7ReceiverPatientOutpatientEncounters(String patientUuid,
                                                                    FhirResourceFiler fhirResourceFiler,
                                                                    BartsCsvHelper csvHelper) throws Exception {

        UUID serviceUuid = fhirResourceFiler.getServiceId();
        UUID systemUuid = fhirResourceFiler.getSystemId();

        //we want to delete any HL7 Encounter more than 24 hours older than the DW file extract date
        Date extractDateTime = csvHelper.getExtractDateTime();
        Date cutoff = new Date(extractDateTime.getTime() - (24 * 60 * 60 * 1000));

        ResourceDalI resourceDal = DalProvider.factoryResourceDal();
        List<ResourceWrapper> resourceWrappers
                = resourceDal.getResourcesByPatient(serviceUuid, UUID.fromString(patientUuid), ResourceType.Encounter.toString());
        for (ResourceWrapper wrapper: resourceWrappers) {

            //if this episode is for our own service + system ID (i.e. DW feed), then leave it
            UUID wrapperSystemId = wrapper.getSystemId();
            if (wrapperSystemId.equals(systemUuid)) {
                continue;
            }

            String json = wrapper.getResourceData();
            Encounter existingEncounter = (Encounter) FhirSerializationHelper.deserializeResource(json);

            //if the HL7 Encounter has no date info at all or is before our cutoff, delete it
            if (!existingEncounter.hasPeriod()
                    || !existingEncounter.getPeriod().hasStart()
                    || existingEncounter.getPeriod().getStart().before(cutoff)) {

                //finally, check it is an Outpatient encounter class before deleting
                if (existingEncounter.getClass_().equals(Encounter.EncounterClass.OUTPATIENT)) {
                    GenericBuilder builder = new GenericBuilder(existingEncounter);
                    //we have no audit for deleting these encounters, since it's not triggered by a specific piece of data
                    //builder.setDeletedAudit(...);
                    fhirResourceFiler.deletePatientResource(null, false, builder);
                }
            }
        }
    }

    private static void setAttendanceContainedParameters(EncounterBuilder encounterBuilder,
                                                        StagingOutpatientCdsTarget targetOutpatientCds) throws Exception {

        ContainedParametersBuilder parametersBuilder = new ContainedParametersBuilder(encounterBuilder);
        parametersBuilder.removeContainedParameters();

        String adminCategoryCode = targetOutpatientCds.getAdministrativeCategoryCode();
        if (!Strings.isNullOrEmpty(adminCategoryCode)) {

            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","outpatient",
                    "administrative_category_code"
            );
            MapResponse propertyResponse = IMClient.getMapProperty(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","outpatient",
                    "administrative_category_code", adminCategoryCode,"CM_NHS_DD"
            );
            MapResponse valueResponse = IMClient.getMapPropertyValue(valueRequest);

            String propertyConceptIri = propertyResponse.getConcept().getIri();
            String valueConceptIri = valueResponse.getConcept().getIri();
            parametersBuilder.addParameter(propertyConceptIri, valueConceptIri);
        }

        String referralSourceId = targetOutpatientCds.getReferralSource();
        if (!Strings.isNullOrEmpty(referralSourceId)) {

            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","outpatient",
                    "referral_source"
            );
            MapResponse propertyResponse = IMClient.getMapProperty(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","outpatient",
                    "referral_source", referralSourceId,"CM_NHS_DD"
            );
            MapResponse valueResponse = IMClient.getMapPropertyValue(valueRequest);

            String propertyConceptIri = propertyResponse.getConcept().getIri();
            String valueConceptIri = valueResponse.getConcept().getIri();
            parametersBuilder.addParameter(propertyConceptIri, valueConceptIri);
        }

        String apptAttendedCode = targetOutpatientCds.getApptAttendedCode();
        if (!Strings.isNullOrEmpty(apptAttendedCode)) {

            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","outpatient",
                    "appt_attended_code"
            );
            MapResponse propertyResponse = IMClient.getMapProperty(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","outpatient",
                    "appt_attended_code", apptAttendedCode,"CM_NHS_DD"
            );
            MapResponse valueResponse = IMClient.getMapPropertyValue(valueRequest);

            String propertyConceptIri = propertyResponse.getConcept().getIri();
            String valueConceptIri = valueResponse.getConcept().getIri();
            parametersBuilder.addParameter(propertyConceptIri, valueConceptIri);
        }

        String apptOutcomeCode = targetOutpatientCds.getApptOutcomeCode();
        if (!Strings.isNullOrEmpty(apptOutcomeCode)) {

            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","outpatient",
                    "appt_outcome_code"
            );
            MapResponse propertyResponse = IMClient.getMapProperty(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","outpatient",
                    "appt_outcome_code", apptOutcomeCode,"CM_NHS_DD"
            );
            MapResponse valueResponse = IMClient.getMapPropertyValue(valueRequest);

            String propertyConceptIri = propertyResponse.getConcept().getIri();
            String valueConceptIri = valueResponse.getConcept().getIri();
            parametersBuilder.addParameter(propertyConceptIri, valueConceptIri);
        }

        String treatmentFunctionCode = targetOutpatientCds.getTreatmentFunctionCode();
        if (!Strings.isNullOrEmpty(treatmentFunctionCode)) {

            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","outpatient",
                    "treatment_function_code"
            );
            MapResponse propertyResponse = IMClient.getMapProperty(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","outpatient",
                    "treatment_function_code", treatmentFunctionCode,"CM_BartCernerCode"
            );
            MapResponse valueResponse = IMClient.getMapPropertyValue(valueRequest);

            String propertyConceptIri = propertyResponse.getConcept().getIri();
            String valueConceptIri = valueResponse.getConcept().getIri();
            parametersBuilder.addParameter(propertyConceptIri, valueConceptIri);
        }
    }
}