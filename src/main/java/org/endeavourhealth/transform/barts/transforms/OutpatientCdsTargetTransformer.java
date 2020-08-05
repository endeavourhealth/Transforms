package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceComponents;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.eds.PatientSearchDalI;
import org.endeavourhealth.core.database.dal.eds.models.PatientSearch;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingOutpatientCdsTarget;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.im.models.mapping.MapColumnRequest;
import org.endeavourhealth.im.models.mapping.MapColumnValueRequest;
import org.endeavourhealth.im.models.mapping.MapResponse;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.subscriber.IMConstant;
import org.endeavourhealth.transform.subscriber.IMHelper;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class OutpatientCdsTargetTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(OutpatientCdsTargetTransformer.class);
    private static Set<Long> patientOutpatientEncounterDates = new HashSet<>();
    private static Set<String> patientOutpatientEpisodesDeleted = new HashSet<>();

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

            patientOutpatientEncounterDates.clear();
            patientOutpatientEpisodesDeleted.clear();

            //process top level encounter - the existing parent encounter
            Integer encounterId = targetOutpatientCds.getEncounterId();  //this is used to identify the top level parent episode
            if (encounterId != null) {

                EncounterBuilder parentEncounterBuilder = null;

                Encounter existingParentEncounter
                        = (Encounter) csvHelper.retrieveResourceForLocalId(ResourceType.Encounter, Integer.toString(encounterId));
                if (existingParentEncounter != null) {

                    //update the top level encounter
                    parentEncounterBuilder
                            = updateExistingEncounter(existingParentEncounter, targetOutpatientCds, fhirResourceFiler, csvHelper);

                    //create the linked child encounter
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
                deleteHL7ReceiverPatientOutpatientEncounters(targetOutpatientCds, fhirResourceFiler, csvHelper);
                patientOutpatientEncounterDates.clear();
                patientOutpatientEpisodesDeleted.clear();

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

        //NOTE: diagnosis and procedure data is already processed via proc and diag CDS transforms

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
        patientOutpatientEncounterDates.add(existingParentEncounterBuilder.getPeriod().getStart().getTime());

        //LOG.debug("Saving child OP encounter: "+FhirSerializationHelper.serializeResource(encounterBuilder.getResource()));
        fhirResourceFiler.savePatientResource(null, encounterBuilder);
        patientOutpatientEncounterDates.add(encounterBuilder.getPeriod().getStart().getTime());
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

    private static EncounterBuilder updateExistingEncounter(Encounter existingEncounter,
                                                StagingOutpatientCdsTarget targetOutpatientCds,
                                                FhirResourceFiler fhirResourceFiler,
                                                BartsCsvHelper csvHelper) throws Exception {

        EncounterBuilder existingEncounterBuilder
                = new EncounterBuilder(existingEncounter, targetOutpatientCds.getAudit());

        //update the parent Encounter status to finished depending on the outcome of latest sub encounter
        String apptOutcomeCode = targetOutpatientCds.getApptOutcomeCode();
        if (apptOutcomeCode.equalsIgnoreCase("1")) {

            existingEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
        }

        String cdsUniqueId = targetOutpatientCds.getUniqueId();
        if (!cdsUniqueId.isEmpty()) {

            cdsUniqueId = cdsUniqueId.replaceFirst("OPCDS-","");
            IdentifierBuilder.removeExistingIdentifiersForSystem(existingEncounterBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CDS_UNIQUE_ID);
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(existingEncounterBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CDS_UNIQUE_ID);
            identifierBuilder.setValue(cdsUniqueId);
        }

        return existingEncounterBuilder;
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

        // Retrieve or create EpisodeOfCare link for encounter including sub-encounters
        EpisodeOfCareBuilder episodeOfCareBuilder
                = csvHelper.getEpisodeOfCareCache().getEpisodeOfCareBuilder(targetOutpatientCds);
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
        Integer performerPersonnelId = targetOutpatientCds.getPerformerPersonnelId();
        if (performerPersonnelId != null && performerPersonnelId != 0) {

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

    /*
     * we match to some HL7 Receiver Encounters, basically taking them over
     * so we call this to tidy up (delete) any matching Encounters that have been taken over
     */
    private static void deleteHL7ReceiverPatientOutpatientEncounters(StagingOutpatientCdsTarget targetOutpatientCds,
                                                                    FhirResourceFiler fhirResourceFiler,
                                                                    BartsCsvHelper csvHelper) throws Exception {

        UUID serviceUuid = fhirResourceFiler.getServiceId();
        UUID systemUuid = fhirResourceFiler.getSystemId();

        //we want to delete HL7 Emergency Encounters more than 24 hours older than the extract data date
        Date extractDateTime = fhirResourceFiler.getDataDate();
        Date cutoff = new Date(extractDateTime.getTime() - (12 * 60 * 60 * 1000));

        String sourcePatientId = Integer.toString(targetOutpatientCds.getPersonId());
        UUID patientUuid = IdHelper.getEdsResourceId(serviceUuid, ResourceType.Patient, sourcePatientId);

        //try to locate the patient to obtain the nhs number
        PatientSearchDalI patientSearchDal = DalProvider.factoryPatientSearchDal();
        PatientSearch patientSearch = patientSearchDal.searchByPatientId(patientUuid);
        if (patientSearch == null) {
            LOG.warn("Cannot find patient using Id: "+patientUuid.toString());
            return;
        }
        String nhsNumber = patientSearch.getNhsNumber();
        Set<String> serviceIds = new HashSet<>();
        serviceIds.add(serviceUuid.toString());
        //get the list of patientId values for this service as Map<patientId, serviceId>
        Map<UUID, UUID> patientIdsForService = patientSearchDal.findPatientIdsForNhsNumber(serviceIds, nhsNumber);
        Set<UUID> patientIds = patientIdsForService.keySet();   //get the unique patientId values, >1 where >1 system

        //loop through all the patientIds for that patient to check the encounters
        for (UUID patientId: patientIds) {

            ResourceDalI resourceDal = DalProvider.factoryResourceDal();
            List<ResourceWrapper> resourceWrappers
                    = resourceDal.getResourcesByPatient(serviceUuid, patientId, ResourceType.Encounter.toString());
            for (ResourceWrapper wrapper : resourceWrappers) {

                //if this Encounter is for our own service + system ID (i.e. DW feed), then leave it
                UUID wrapperSystemId = wrapper.getSystemId();
                if (wrapperSystemId.equals(systemUuid)) {
                    continue;
                }

                String json = wrapper.getResourceData();
                Encounter existingEncounter = (Encounter) FhirSerializationHelper.deserializeResource(json);

                //LOG.debug("Existing HL7 Outpatient encounter " + existingEncounter.getId() + ", date: " + existingEncounter.getPeriod().getStart().toString() + ", cut off date: " + cutoff.toString());

                //if the HL7 Encounter is before our 24 hr cutoff, look to delete it
                if (existingEncounter.hasPeriod()
                        && existingEncounter.getPeriod().hasStart()
                        && existingEncounter.getPeriod().getStart().before(cutoff)) {

                    //LOG.debug("Checking existing Outpatient encounter date (long): " + existingEncounter.getPeriod().getStart().getTime() + " in dates array: " + patientOutpatientEncounterDates.toArray());

                    //finally, check it is an Outpatient encounter class before deleting
                    if (existingEncounter.getClass_().equals(Encounter.EncounterClass.OUTPATIENT)) {
                        if (patientOutpatientEncounterDates.contains(existingEncounter.getPeriod().getStart().getTime())) {
                            GenericBuilder builder = new GenericBuilder(existingEncounter);
                            //we have no audit for deleting these encounters, since it's not triggered by a specific piece of data
                            //builder.setDeletedAudit(...);
                            LOG.debug("Existing Outpatient ADT encounterId: "+existingEncounter.getId()+" deleted as matched type and date to DW");
                            fhirResourceFiler.deletePatientResource(null, false, builder);

                            //get the linked episode of care reference and delete the resource so duplication does not occur between DW and ADT
                            if (existingEncounter.hasEpisodeOfCare()) {
                                Reference episodeReference = existingEncounter.getEpisodeOfCare().get(0);
                                String episodeUuid = ReferenceHelper.getReferenceId(episodeReference);

                                //add episode of care for deletion if not already deleted
                                if (patientOutpatientEpisodesDeleted.contains(episodeUuid)) {
                                    continue;
                                }
                                EpisodeOfCare episodeOfCare
                                        = (EpisodeOfCare)resourceDal.getCurrentVersionAsResource(serviceUuid, ResourceType.EpisodeOfCare, episodeUuid);

                                if (episodeOfCare != null) {
                                    GenericBuilder builderEpisode = new GenericBuilder(episodeOfCare);

                                    patientOutpatientEpisodesDeleted.add(episodeUuid);
                                    fhirResourceFiler.deletePatientResource(null, false, builderEpisode);
                                    LOG.debug("Existing Emergency ADT episodeId: " + episodeUuid + " deleted as linked to deleted encounter");
                                }
                            }
                        }
                    }
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
            MapResponse propertyResponse = IMHelper.getIMMappedPropertyResponse(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","outpatient",
                    "administrative_category_code", adminCategoryCode, IMConstant.NHS_DATA_DICTIONARY
            );
            MapResponse valueResponse = IMHelper.getIMMappedPropertyValueResponse(valueRequest);

            CodeableConcept ccValue = new CodeableConcept();
            ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                    .setSystem(valueResponse.getConcept().getScheme());
            parametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
        }

        String referralSourceId = targetOutpatientCds.getReferralSource();
        if (!Strings.isNullOrEmpty(referralSourceId)) {

            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","outpatient",
                    "referral_source"
            );
            MapResponse propertyResponse = IMHelper.getIMMappedPropertyResponse(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","outpatient",
                    "referral_source", referralSourceId, IMConstant.NHS_DATA_DICTIONARY
            );
            MapResponse valueResponse = IMHelper.getIMMappedPropertyValueResponse(valueRequest);

            CodeableConcept ccValue = new CodeableConcept();
            ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                    .setSystem(valueResponse.getConcept().getScheme());
            parametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
        }

        String apptAttendedCode = targetOutpatientCds.getApptAttendedCode();
        if (!Strings.isNullOrEmpty(apptAttendedCode)) {

            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","outpatient",
                    "appt_attended_code"
            );
            MapResponse propertyResponse = IMHelper.getIMMappedPropertyResponse(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","outpatient",
                    "appt_attended_code", apptAttendedCode, IMConstant.NHS_DATA_DICTIONARY
            );
            MapResponse valueResponse = IMHelper.getIMMappedPropertyValueResponse(valueRequest);

            CodeableConcept ccValue = new CodeableConcept();
            ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                    .setSystem(valueResponse.getConcept().getScheme());
            parametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
        }

        String apptOutcomeCode = targetOutpatientCds.getApptOutcomeCode();
        if (!Strings.isNullOrEmpty(apptOutcomeCode)) {

            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","outpatient",
                    "appt_outcome_code"
            );
            MapResponse propertyResponse = IMHelper.getIMMappedPropertyResponse(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","outpatient",
                    "appt_outcome_code", apptOutcomeCode, IMConstant.NHS_DATA_DICTIONARY
            );
            MapResponse valueResponse = IMHelper.getIMMappedPropertyValueResponse(valueRequest);

            CodeableConcept ccValue = new CodeableConcept();
            ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                    .setSystem(valueResponse.getConcept().getScheme());
            parametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
        }

        String treatmentFunctionCode = targetOutpatientCds.getTreatmentFunctionCode();
        if (!Strings.isNullOrEmpty(treatmentFunctionCode) && !treatmentFunctionCode.equals("0")) {

            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","outpatient",
                    "treatment_function_code"
            );
            MapResponse propertyResponse = IMHelper.getIMMappedPropertyResponse(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","outpatient",
                    "treatment_function_code", treatmentFunctionCode,IMConstant.BARTS_CERNER
            );
            MapResponse valueResponse = IMHelper.getIMMappedPropertyValueResponse(valueRequest);

            CodeableConcept ccValue = new CodeableConcept();
            ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                    .setSystem(valueResponse.getConcept().getScheme());
            parametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
        }
    }
}