package org.endeavourhealth.transform.barts.transforms;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Strings;
import com.google.gson.JsonObject;
import org.endeavourhealth.common.cache.ObjectMapperPool;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceComponents;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.EncounterParticipantType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.eds.PatientSearchDalI;
import org.endeavourhealth.core.database.dal.eds.models.PatientSearch;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingInpatientCdsTarget;
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

public class InpatientCdsTargetTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(InpatientCdsTargetTransformer.class);
    private static Set<Long> patientInpatientEncounterDates = new HashSet<>();
    private static Set<String> patientInpatientEpisodesDeleted = new HashSet<>();

    public static void transform(FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        try {
            createInpatientCdsEncounters(fhirResourceFiler, csvHelper);
        } catch (Exception ex) {
            fhirResourceFiler.logTransformRecordError(ex, null);
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }


    public static void createInpatientCdsEncounters(FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        // retrieve the target inpatient cds records for the current exchangeId
        List<StagingInpatientCdsTarget> targetInpatientCdsRecords = csvHelper.retrieveTargetInpatientCds();
        if (targetInpatientCdsRecords == null) {
            return;
        }

        for (StagingInpatientCdsTarget targetInpatientCds : targetInpatientCdsRecords) {

            //check if any patient filtering applied
            String personId = Integer.toString(targetInpatientCds.getPersonId());
            if (!csvHelper.processRecordFilteringOnPatientId(personId)) {
                continue;
            }

            boolean isDeleted = targetInpatientCds.isDeleted();
            if (isDeleted) {

                deleteInpatientCdsEncounterAndChildren(targetInpatientCds, fhirResourceFiler, csvHelper);
                continue;
            }

            patientInpatientEncounterDates.clear();
            patientInpatientEpisodesDeleted.clear();

            //process top level encounter - the existing parent encounter
            Integer encounterId = targetInpatientCds.getEncounterId();  //this is used to identify the top level parent encounter

            if (encounterId != null) {

                EncounterBuilder parentEncounterBuilder = null;

                Encounter existingParentEncounter
                        = (Encounter) csvHelper.retrieveResourceForLocalId(ResourceType.Encounter, Integer.toString(encounterId));
                if (existingParentEncounter != null) {

                    //update the existing top level encounter
                    parentEncounterBuilder
                            = updateExistingParentEncounter(existingParentEncounter, targetInpatientCds, fhirResourceFiler, csvHelper);

                    //create the linked child encounters
                    createInpatientCdsSubEncounters(targetInpatientCds, fhirResourceFiler, csvHelper, parentEncounterBuilder);

                } else {

                    //create top level parent with minimum data including the sub encounters
                    parentEncounterBuilder
                            = createInpatientCdsEncounterParentAndSubs(targetInpatientCds, fhirResourceFiler, csvHelper);
                }

                //now delete any older HL7 Encounters for patients we've updated
                //but waiting until everything has been saved to the DB first
                fhirResourceFiler.waitUntilEverythingIsSaved();

                //find the patient UUID for the encounters we have just filed, so we can tidy up the
                //HL7 encounters after doing all the saving of the DW encounters
                deleteHL7ReceiverPatientInpatientEncounters(targetInpatientCds, fhirResourceFiler, csvHelper);
                patientInpatientEncounterDates.clear();
                patientInpatientEpisodesDeleted.clear();

            } else {

                String uniqueId = targetInpatientCds.getUniqueId();
                //throw new Exception("encounter_id missing for Inpatient CDS record: " + uniqueId);
                LOG.warn("encounter_id missing for Inpatient CDS record: " + uniqueId);
            }
        }
    }

    private static void setCommonEncounterAttributes(EncounterBuilder builder,
                                                     StagingInpatientCdsTarget targetInpatientCds,
                                                     BartsCsvHelper csvHelper,
                                                     boolean isChildEncounter,
                                                     FhirResourceFiler fhirResourceFiler) throws Exception {

        //every encounter has the following common attributes
        Integer personId = targetInpatientCds.getPersonId();
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
                = csvHelper.getEpisodeOfCareCache().getEpisodeOfCareBuilder(targetInpatientCds);
        if (episodeOfCareBuilder != null) {

            csvHelper.setEpisodeReferenceOnEncounter(episodeOfCareBuilder, builder, fhirResourceFiler);

            Date spellStartDate = targetInpatientCds.getDtSpellStart();
            Date dischargeDate = targetInpatientCds.getDtDischarge();
            if (spellStartDate != null) {

                //if this episode started in A&E this will not be updated
                if (episodeOfCareBuilder.getRegistrationStartDate() == null || spellStartDate.before(episodeOfCareBuilder.getRegistrationStartDate())) {
                    episodeOfCareBuilder.setRegistrationStartDate(spellStartDate);
                    episodeOfCareBuilder.setStatus(EpisodeOfCare.EpisodeOfCareStatus.ACTIVE);
                }

                // End date
                if (dischargeDate != null) {
                    if (episodeOfCareBuilder.getRegistrationEndDate() == null || dischargeDate.after(episodeOfCareBuilder.getRegistrationEndDate())) {
                        episodeOfCareBuilder.setRegistrationEndDate(dischargeDate);
                    }
                }
            } else {
                if (episodeOfCareBuilder.getRegistrationEndDate() == null) {
                    episodeOfCareBuilder.setStatus(EpisodeOfCare.EpisodeOfCareStatus.PLANNED);
                }
            }
        }

        Integer performerPersonnelId = targetInpatientCds.getPerformerPersonnelId();
        if (performerPersonnelId != null && performerPersonnelId != 0) {

            Reference practitionerReference
                    = ReferenceHelper.createReference(ResourceType.Practitioner, Integer.toString(performerPersonnelId));
            if (builder.isIdMapped()) {

                practitionerReference
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(practitionerReference, csvHelper);
            }
            builder.addParticipant(practitionerReference, EncounterParticipantType.PRIMARY_PERFORMER);
        }
        String serviceProviderOrgId = targetInpatientCds.getEpisodeStartSiteCode();
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

            Integer encounterId = targetInpatientCds.getEncounterId();
            Reference parentEncounter
                    = ReferenceHelper.createReference(ResourceType.Encounter, Integer.toString(encounterId));
            parentEncounter = IdHelper.convertLocallyUniqueReferenceToEdsReference(parentEncounter, csvHelper);
            builder.setPartOf(parentEncounter);
        }
        //set the CDS identifier against the Encounter
        String cdsUniqueId = targetInpatientCds.getUniqueId();
        if (!cdsUniqueId.isEmpty()) {

            cdsUniqueId = cdsUniqueId.replaceFirst("IPCDS-","");
            IdentifierBuilder.removeExistingIdentifiersForSystem(builder, FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CDS_UNIQUE_ID);
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(builder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CDS_UNIQUE_ID);
            identifierBuilder.setValue(cdsUniqueId);
        }
    }

    private static void createInpatientCdsSubEncounters(StagingInpatientCdsTarget targetInpatientCds,
                                                        FhirResourceFiler fhirResourceFiler,
                                                        BartsCsvHelper csvHelper,
                                                        EncounterBuilder existingParentEncounterBuilder) throws Exception {

        //unique to the inpatient hospital spell
        String spellId = targetInpatientCds.getSpellNumber();

        //the episode number ranges from 01 (Admission plus episode start / finish), 02, 03 till discharge
        String episodeNumber = targetInpatientCds.getEpisodeNumber();

        //retrieve the parent encounter (if not passed in) to point to any new child encounters created during this method
        ContainedListBuilder existingParentEncounterList;
        if (existingParentEncounterBuilder == null) {

            Integer parentEncounterId = targetInpatientCds.getEncounterId();
            Encounter existingParentEncounter
                    = (Encounter) csvHelper.retrieveResourceForLocalId(ResourceType.Encounter, Integer.toString(parentEncounterId));
            existingParentEncounterBuilder = new EncounterBuilder(existingParentEncounter, targetInpatientCds.getAudit());
        }
        existingParentEncounterList = new ContainedListBuilder(existingParentEncounterBuilder);

        EncounterBuilder admissionEncounterBuilder = null;
        EncounterBuilder dischargeEncounterBuilder = null;

        //episodeNumber = 01 then create the inpatient admission and the discharge encounters (if date set)
        if (episodeNumber.equalsIgnoreCase("01")) {

            admissionEncounterBuilder = new EncounterBuilder(null, targetInpatientCds.getAudit());
            admissionEncounterBuilder.setClass(Encounter.EncounterClass.INPATIENT);

            String admissionEncounterId = spellId + ":01:IP:Admission";
            admissionEncounterBuilder.setId(admissionEncounterId);
            Date spellStartDate = targetInpatientCds.getDtSpellStart();
            admissionEncounterBuilder.setPeriodStart(spellStartDate);
            admissionEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);

            CodeableConceptBuilder codeableConceptBuilderAdmission
                    = new CodeableConceptBuilder(admissionEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
            codeableConceptBuilderAdmission.setText("Inpatient Admission");

            setCommonEncounterAttributes(admissionEncounterBuilder, targetInpatientCds, csvHelper, true, fhirResourceFiler);

            //add in additional extended data as Parameters resource with additional extension
            setAdmissionContainedParameters(admissionEncounterBuilder, targetInpatientCds);

            //if the 01 episode has an episode start date, set the admission status to finished and end date
            Date episodeStartDate = targetInpatientCds.getDtEpisodeStart();
            if (episodeStartDate != null) {

                admissionEncounterBuilder.setPeriodEnd(episodeStartDate);
                admissionEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
            }

            //and link the parent to this new child encounter
            Reference childAdmissionRef = ReferenceHelper.createReference(ResourceType.Encounter, admissionEncounterId);
            if (existingParentEncounterBuilder.isIdMapped()) {

                childAdmissionRef
                        = IdHelper.convertLocallyUniqueReferenceToEdsReference(childAdmissionRef, csvHelper);
            }
            existingParentEncounterList.addReference(childAdmissionRef);

            //the main encounter has a discharge date so set the end date and create a linked Discharge encounter
            Date spellDischargeDate = targetInpatientCds.getDtDischarge();
            if (spellDischargeDate != null) {

                //create new additional Discharge encounter event to link to the top level parent
                dischargeEncounterBuilder = new EncounterBuilder(null, targetInpatientCds.getAudit());
                dischargeEncounterBuilder.setClass(Encounter.EncounterClass.INPATIENT);

                String dischargeEncounterId = spellId + ":01:IP:Discharge";
                dischargeEncounterBuilder.setId(dischargeEncounterId);
                dischargeEncounterBuilder.setPeriodStart(spellDischargeDate);
                dischargeEncounterBuilder.setPeriodEnd(spellDischargeDate);
                dischargeEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);

                CodeableConceptBuilder codeableConceptBuilderDischarge
                        = new CodeableConceptBuilder(dischargeEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
                codeableConceptBuilderDischarge.setText("Inpatient Discharge");

                setCommonEncounterAttributes(dischargeEncounterBuilder, targetInpatientCds, csvHelper, true, fhirResourceFiler);

                //add in additional extended data as Parameters resource with additional extension
                setDischargeContainedParameters(dischargeEncounterBuilder, targetInpatientCds);

                //and link the parent to this new child encounter
                Reference childDischargeRef = ReferenceHelper.createReference(ResourceType.Encounter, dischargeEncounterId);
                if (existingParentEncounterBuilder.isIdMapped()) {

                    childDischargeRef
                            = IdHelper.convertLocallyUniqueReferenceToEdsReference(childDischargeRef, csvHelper);
                }

                existingParentEncounterList.addReference(childDischargeRef);
            }
        }

        //these are the 01, 02, 03, 04 subsequent episodes where activity happens, maternity, wards change etc.
        //also, critical care child encounters link back to these as their parents via their own transform
        EncounterBuilder episodeEncounterBuilder = new EncounterBuilder(null, targetInpatientCds.getAudit());
        episodeEncounterBuilder.setClass(Encounter.EncounterClass.INPATIENT);

        String episodeEncounterId = spellId +":"+episodeNumber+":IP:Episode";
        episodeEncounterBuilder.setId(episodeEncounterId);

        //spell episode encounter have their own start and end date/times
        Date episodeStartDate = targetInpatientCds.getDtEpisodeStart();
        episodeEncounterBuilder.setPeriodStart(episodeStartDate);

        Date episodeEndDate = targetInpatientCds.getDtEpisodeEnd();
        if (episodeEndDate != null) {

            episodeEncounterBuilder.setPeriodEnd(episodeEndDate);
            episodeEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
        } else {
            episodeEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);
        }

        CodeableConceptBuilder codeableConceptBuilder
                = new CodeableConceptBuilder(episodeEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
        codeableConceptBuilder.setText("Inpatient Episode");

        setCommonEncounterAttributes(episodeEncounterBuilder, targetInpatientCds, csvHelper, true, fhirResourceFiler);

        //and link the parent to this new child encounter
        Reference childEpisodeRef = ReferenceHelper.createReference(ResourceType.Encounter, episodeEncounterId);
        if (existingParentEncounterBuilder.isIdMapped()) {

            childEpisodeRef
                    = IdHelper.convertLocallyUniqueReferenceToEdsReference(childEpisodeRef, csvHelper);
        }
        existingParentEncounterList.addReference(childEpisodeRef);

        setEpisodeContainedParameters(episodeEncounterBuilder, targetInpatientCds);

        //save the existing parent encounter here with the updated child refs added during this method, then the sub encounters
        //LOG.debug("Saving IP parent encounter: "+ FhirSerializationHelper.serializeResource(existingParentEpisodeBuilder.getResource()));
        fhirResourceFiler.savePatientResource(null, !existingParentEncounterBuilder.isIdMapped(), existingParentEncounterBuilder);
        patientInpatientEncounterDates.add(existingParentEncounterBuilder.getPeriod().getStart().getTime());

        //then save the child encounter builders if they are set
        if (admissionEncounterBuilder != null) {

            //LOG.debug("Saving child IP admission encounter: "+ FhirSerializationHelper.serializeResource(admissionEncounterBuilder.getResource()));
            fhirResourceFiler.savePatientResource(null, admissionEncounterBuilder);
            patientInpatientEncounterDates.add(admissionEncounterBuilder.getPeriod().getStart().getTime());
        }
        if (dischargeEncounterBuilder != null) {

            //LOG.debug("Saving child IP discharge encounter: "+ FhirSerializationHelper.serializeResource(dischargeEncounterBuilder.getResource()));
            fhirResourceFiler.savePatientResource(null, dischargeEncounterBuilder);
            patientInpatientEncounterDates.add(dischargeEncounterBuilder.getPeriod().getStart().getTime());
        }
        //finally, save the episode encounter which always exists
        //LOG.debug("Saving child IP episode encounter: "+ FhirSerializationHelper.serializeResource(episodeEncounterBuilder.getResource()));
        fhirResourceFiler.savePatientResource(null, episodeEncounterBuilder);
        patientInpatientEncounterDates.add(episodeEncounterBuilder.getPeriod().getStart().getTime());
    }

    private static void deleteInpatientCdsEncounterAndChildren(StagingInpatientCdsTarget targetInpatientCds,
                                                                FhirResourceFiler fhirResourceFiler,
                                                                BartsCsvHelper csvHelper) throws Exception {

        Integer encounterId = targetInpatientCds.getEncounterId();  //this is used to identify the top level parent episode

        //retrieve the existing Top level parent Encounter resource to perform a deletion plus any child encounters
        if (encounterId != null) {

            Encounter existingParentEncounter
                    = (Encounter) csvHelper.retrieveResourceForLocalId(ResourceType.Encounter, Integer.toString(encounterId));

            if (existingParentEncounter != null) {

                EncounterBuilder parentEncounterBuilder
                        = new EncounterBuilder(existingParentEncounter, targetInpatientCds.getAudit());

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

                            fhirResourceFiler.deletePatientResource(null, false, new EncounterBuilder(childEncounter, targetInpatientCds.getAudit()));
                        } else {

                            TransformWarnings.log(LOG, csvHelper, "Cannot find existing child Encounter ref: {} for deletion", comps.getId());
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

    private static EncounterBuilder createInpatientCdsEncounterParentAndSubs(StagingInpatientCdsTarget targetInpatientCds,
                                                                    FhirResourceFiler fhirResourceFiler,
                                                                    BartsCsvHelper csvHelper) throws Exception {

        EncounterBuilder parentEncounterBuilder = new EncounterBuilder(null, targetInpatientCds.getAudit());
        parentEncounterBuilder.setClass(Encounter.EncounterClass.INPATIENT);

        Integer encounterId = targetInpatientCds.getEncounterId();
        parentEncounterBuilder.setId(Integer.toString(encounterId));

        Date spellStartDate = targetInpatientCds.getDtSpellStart();
        parentEncounterBuilder.setPeriodStart(spellStartDate);

        Date dischargeDate = targetInpatientCds.getDtDischarge();
        if (dischargeDate != null) {

            parentEncounterBuilder.setPeriodEnd(dischargeDate);
            parentEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
        } else {

            parentEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);
        }

        CodeableConceptBuilder codeableConceptBuilder
                = new CodeableConceptBuilder(parentEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
        codeableConceptBuilder.setText("Inpatient");

        setCommonEncounterAttributes(parentEncounterBuilder, targetInpatientCds, csvHelper, false, fhirResourceFiler);

        //once the parent is populated, then create the sub encounters, passing through the parent for linking
        createInpatientCdsSubEncounters(targetInpatientCds, fhirResourceFiler, csvHelper, parentEncounterBuilder);

        return parentEncounterBuilder;
    }

    private static EncounterBuilder updateExistingParentEncounter(Encounter existingEncounter,
                                                StagingInpatientCdsTarget targetInpatientCds,
                                                FhirResourceFiler fhirResourceFiler,
                                                BartsCsvHelper csvHelper) throws Exception {

        EncounterBuilder existingEncounterBuilder
                = new EncounterBuilder(existingEncounter, targetInpatientCds.getAudit());

        //this ensures the type is always an Inpatient and handles the scenario when an Emergency parent encounter
        //becomes an Inpatient Encounter stay
        CodeableConceptBuilder codeableConceptBuilder
                = new CodeableConceptBuilder(existingEncounterBuilder, CodeableConceptBuilder.Tag.Encounter_Source);
        codeableConceptBuilder.setText("Inpatient");
        existingEncounterBuilder.setClass(Encounter.EncounterClass.INPATIENT);

        //set the overall encounter status depending on sub encounter completion
        Date spellStartDate = targetInpatientCds.getDtSpellStart();
        Date dischargeDate = targetInpatientCds.getDtDischarge();
        if (spellStartDate != null) {

            existingEncounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS);

            // End date
            if (dischargeDate != null) {

                existingEncounterBuilder.setPeriodEnd(dischargeDate);
                existingEncounterBuilder.setStatus(Encounter.EncounterState.FINISHED);
            }
        } else {

                existingEncounterBuilder.setStatus(Encounter.EncounterState.PLANNED);
        }

        String cdsUniqueId = targetInpatientCds.getUniqueId();
        if (!cdsUniqueId.isEmpty()) {

            cdsUniqueId = cdsUniqueId.replaceFirst("IPCDS-","");
            IdentifierBuilder.removeExistingIdentifiersForSystem(existingEncounterBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CDS_UNIQUE_ID);
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(existingEncounterBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_CDS_UNIQUE_ID);
            identifierBuilder.setValue(cdsUniqueId);
        }

        return existingEncounterBuilder;
    }

    /*
     * we match to some HL7 Receiver Encounters, basically taking them over
     * so we call this to tidy up (delete) any matching Encounters that have been taken over
     */
    private static void deleteHL7ReceiverPatientInpatientEncounters(StagingInpatientCdsTarget targetInpatientCds,
                                                                     FhirResourceFiler fhirResourceFiler,
                                                                     BartsCsvHelper csvHelper) throws Exception {

        UUID serviceUuid = fhirResourceFiler.getServiceId();
        UUID systemUuid = fhirResourceFiler.getSystemId();

        //we want to check for and delete HL7 Inpatient Encounters more than 12 hours older than the DW extract data date
        Date extractDateTime = fhirResourceFiler.getDataDate();
        Date cutoff = new Date(extractDateTime.getTime() - (12 * 60 * 60 * 1000));

        String sourcePatientId = Integer.toString(targetInpatientCds.getPersonId());
        UUID patientUuid = IdHelper.getEdsResourceId(serviceUuid, ResourceType.Patient, sourcePatientId);

        //try to locate the patient to obtain the nhs number
        PatientSearchDalI patientSearchDal = DalProvider.factoryPatientSearchDal();
        PatientSearch patientSearch = patientSearchDal.searchByPatientId(patientUuid);
        if (patientSearch == null) {
            LOG.warn("Cannot find patient using Id: "+patientUuid.toString());
            return;
        }
        String nhsNumber = patientSearch.getNhsNumber();
        Set<UUID> serviceIds = new HashSet<>();
        serviceIds.add(serviceUuid);
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

                //if the HL7 Encounter is before our 12 hr cutoff, look to delete it
                if (existingEncounter.hasPeriod()
                        && existingEncounter.getPeriod().hasStart()
                        && existingEncounter.getPeriod().getStart().before(cutoff)) {

                    //finally, check it is an Inpatient encounter class before deleting
                    if (existingEncounter.getClass_().equals(Encounter.EncounterClass.INPATIENT)) {

                        if (patientInpatientEncounterDates.contains(existingEncounter.getPeriod().getStart().getTime())) {
                            GenericBuilder builderEncounter = new GenericBuilder(existingEncounter);
                            //we have no audit for deleting these encounters, since it's not triggered by a specific piece of data
                            //builder.setDeletedAudit(...);

                            LOG.debug("Existing Inpatient ADT encounterId: "+existingEncounter.getId()+" deleted as matched type and date to DW");
                            fhirResourceFiler.deletePatientResource(null, false, builderEncounter);

                            //get the linked episode of care reference and delete the resource so duplication does not occur between DW and ADT
                            if (existingEncounter.hasEpisodeOfCare()) {
                                Reference episodeReference = existingEncounter.getEpisodeOfCare().get(0);
                                String episodeUuid = ReferenceHelper.getReferenceId(episodeReference);

                                //add episode of care for deletion if not already deleted
                                if (patientInpatientEpisodesDeleted.contains(episodeUuid)) {
                                    continue;
                                }
                                EpisodeOfCare episodeOfCare
                                        = (EpisodeOfCare)resourceDal.getCurrentVersionAsResource(serviceUuid, ResourceType.EpisodeOfCare, episodeUuid);

                                if (episodeOfCare != null) {
                                    GenericBuilder builderEpisode = new GenericBuilder(episodeOfCare);

                                    patientInpatientEpisodesDeleted.add(episodeUuid);
                                    fhirResourceFiler.deletePatientResource(null, false, builderEpisode);
                                    LOG.debug("Existing Inpatient ADT episodeId: " + episodeUuid + " deleted as linked to deleted encounter");
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private static void setAdmissionContainedParameters(EncounterBuilder encounterBuilder,
                                                         StagingInpatientCdsTarget targetInpatientCds) throws Exception {

        ContainedParametersBuilder parametersBuilder = new ContainedParametersBuilder(encounterBuilder);
        parametersBuilder.removeContainedParameters();

        String adminCategoryCode = targetInpatientCds.getAdministrativeCategoryCode();
        if (!Strings.isNullOrEmpty(adminCategoryCode)) {

            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","inpatient",
                    "administrative_category_code"
            );
            MapResponse propertyResponse = IMHelper.getIMMappedPropertyResponse(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","inpatient",
                    "administrative_category_code", adminCategoryCode, IMConstant.NHS_DATA_DICTIONARY
            );
            MapResponse valueResponse = IMHelper.getIMMappedPropertyValueResponse(valueRequest);

            CodeableConcept ccValue = new CodeableConcept();
            ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                    .setSystem(valueResponse.getConcept().getScheme());
            parametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
        }

        String admissionMethodCode = targetInpatientCds.getAdmissionMethodCode();
        if (!Strings.isNullOrEmpty(admissionMethodCode)) {

            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","inpatient",
                    "admission_method_code"
            );
            MapResponse propertyResponse = IMHelper.getIMMappedPropertyResponse(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","inpatient",
                    "admission_method_code", admissionMethodCode, IMConstant.NHS_DATA_DICTIONARY
            );
            MapResponse valueResponse = IMHelper.getIMMappedPropertyValueResponse(valueRequest);

            CodeableConcept ccValue = new CodeableConcept();
            ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                    .setSystem(valueResponse.getConcept().getScheme());
            parametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
        }

        String admissionSourceCode = targetInpatientCds.getAdmissionSourceCode();
        if (!Strings.isNullOrEmpty(admissionSourceCode)) {

            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","inpatient",
                    "admission_source_code"
            );
            MapResponse propertyResponse = IMHelper.getIMMappedPropertyResponse(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","inpatient",
                    "admission_source_code", admissionSourceCode, IMConstant.NHS_DATA_DICTIONARY
            );
            MapResponse valueResponse = IMHelper.getIMMappedPropertyValueResponse(valueRequest);

            CodeableConcept ccValue = new CodeableConcept();
            ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                    .setSystem(valueResponse.getConcept().getScheme());
            parametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
        }

        String patientClassification = targetInpatientCds.getPatientClassification();
        if (!Strings.isNullOrEmpty(patientClassification)) {

            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","inpatient",
                    "patient_classification"
            );
            MapResponse propertyResponse = IMHelper.getIMMappedPropertyResponse(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","inpatient",
                    "patient_classification", patientClassification, IMConstant.NHS_DATA_DICTIONARY
            );
            MapResponse valueResponse = IMHelper.getIMMappedPropertyValueResponse(valueRequest);

            CodeableConcept ccValue = new CodeableConcept();
            ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                    .setSystem(valueResponse.getConcept().getScheme());
            parametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
        }

        String treatmentFunctionCode = targetInpatientCds.getTreatmentFunctionCode();
        if (!Strings.isNullOrEmpty(treatmentFunctionCode) && !treatmentFunctionCode.equals("0")) {

            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","inpatient",
                    "treatment_function_code"
            );
            MapResponse propertyResponse = IMHelper.getIMMappedPropertyResponse(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","inpatient",
                    "treatment_function_code", treatmentFunctionCode, IMConstant.BARTS_CERNER
            );
            MapResponse valueResponse = IMHelper.getIMMappedPropertyValueResponse(valueRequest);

            CodeableConcept ccValue = new CodeableConcept();
            ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                    .setSystem(valueResponse.getConcept().getScheme());
            parametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
        }
    }

    private static void setDischargeContainedParameters(EncounterBuilder encounterBuilder,
                                                        StagingInpatientCdsTarget targetInpatientCds) throws Exception {

        ContainedParametersBuilder parametersBuilder = new ContainedParametersBuilder(encounterBuilder);
        parametersBuilder.removeContainedParameters();

        String dischargeMethodCode = targetInpatientCds.getDischargeMethod();
        if (!Strings.isNullOrEmpty(dischargeMethodCode)) {

            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","inpatient",
                    "discharge_method"
            );
            MapResponse propertyResponse = IMHelper.getIMMappedPropertyResponse(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","inpatient",
                    "discharge_method", dischargeMethodCode, IMConstant.NHS_DATA_DICTIONARY
            );
            MapResponse valueResponse = IMHelper.getIMMappedPropertyValueResponse(valueRequest);

            CodeableConcept ccValue = new CodeableConcept();
            ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                    .setSystem(valueResponse.getConcept().getScheme());
            parametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
        }

        String dischargeDestinationCode = targetInpatientCds.getDischargeDestinationCode();
        if (!Strings.isNullOrEmpty(dischargeDestinationCode)) {

            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","inpatient",
                    "discharge_destination_code"
            );
            MapResponse propertyResponse = IMHelper.getIMMappedPropertyResponse(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","inpatient",
                    "discharge_destination_code", dischargeDestinationCode, IMConstant.NHS_DATA_DICTIONARY
            );
            MapResponse valueResponse = IMHelper.getIMMappedPropertyValueResponse(valueRequest);

            CodeableConcept ccValue = new CodeableConcept();
            ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                    .setSystem(valueResponse.getConcept().getScheme());
            parametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
        }
    }

    private static void setEpisodeContainedParameters(EncounterBuilder encounterBuilder,
                                                        StagingInpatientCdsTarget targetInpatientCds) throws Exception {

        ContainedParametersBuilder parametersBuilder = new ContainedParametersBuilder(encounterBuilder);
        parametersBuilder.removeContainedParameters();

        //assign the additional JSON data maternity to episode 1.  The data is already JSON.
        String episodeNumber = targetInpatientCds.getEpisodeNumber();
        if (episodeNumber.equalsIgnoreCase("01")) {

            //maternity data is either about the baby (maternityBirth - birth) or the mother (maternityDelivery - births(s))
            String maternityBirthJson = targetInpatientCds.getMaternityDataBirth();
            if (!Strings.isNullOrEmpty(maternityBirthJson)) {

                //store the full json first
                parametersBuilder.addParameter("JSON_maternity_birth", maternityBirthJson);

                //we can also save the IM coded data as encounter additional.  This is only possible for the birth
                //record as a single encounter to birth ratio
                //"delivery_method" : "0",
                //"gender" : "2",
                //"live_or_still_birth_indicator" : "1"

                JsonNode maternityJsonNode = ObjectMapperPool.getInstance().readTree(maternityBirthJson);

                JsonNode deliveryNode = maternityJsonNode.get("delivery_method");
                if (deliveryNode != null) {

                    MapColumnRequest propertyRequest = new MapColumnRequest(
                            "CM_Org_Barts", "CM_Sys_Cerner", "CDS", "inpatient",
                            "delivery_method"
                    );
                    MapResponse propertyResponse = IMHelper.getIMMappedPropertyResponse(propertyRequest);

                    MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                            "CM_Org_Barts", "CM_Sys_Cerner", "CDS", "inpatient",
                            "delivery_method", deliveryNode.asText(), IMConstant.NHS_DATA_DICTIONARY
                    );
                    MapResponse valueResponse = IMHelper.getIMMappedPropertyValueResponse(valueRequest);

                    CodeableConcept ccValue = new CodeableConcept();
                    ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                            .setSystem(valueResponse.getConcept().getScheme());
                    parametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
                }

                JsonNode genderNode = maternityJsonNode.get("gender");
                if (genderNode != null) {

                    MapColumnRequest propertyRequest = new MapColumnRequest(
                            "CM_Org_Barts", "CM_Sys_Cerner", "CDS", "inpatient",
                            "gender"
                    );
                    MapResponse propertyResponse = IMHelper.getIMMappedPropertyResponse(propertyRequest);

                    MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                            "CM_Org_Barts", "CM_Sys_Cerner", "CDS", "inpatient",
                            "gender", genderNode.asText(), IMConstant.NHS_DATA_DICTIONARY
                    );
                    MapResponse valueResponse = IMHelper.getIMMappedPropertyValueResponse(valueRequest);

                    CodeableConcept ccValue = new CodeableConcept();
                    ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                            .setSystem(valueResponse.getConcept().getScheme());
                    parametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
                }

                JsonNode liveOrStillNode = maternityJsonNode.get("live_or_still_birth_indicator");
                if (liveOrStillNode != null) {

                    MapColumnRequest propertyRequest = new MapColumnRequest(
                            "CM_Org_Barts", "CM_Sys_Cerner", "CDS", "inpatient",
                            "live_or_still_birth_indicator"
                    );
                    MapResponse propertyResponse = IMHelper.getIMMappedPropertyResponse(propertyRequest);

                    MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                            "CM_Org_Barts", "CM_Sys_Cerner", "CDS", "inpatient",
                            "live_or_still_birth_indicator", liveOrStillNode.asText(), IMConstant.NHS_DATA_DICTIONARY
                    );
                    MapResponse valueResponse = IMHelper.getIMMappedPropertyValueResponse(valueRequest);

                    CodeableConcept ccValue = new CodeableConcept();
                    ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                            .setSystem(valueResponse.getConcept().getScheme());
                    parametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
                }
            }

            String maternityDelivery = targetInpatientCds.getMaternityDataDelivery();
            if (!Strings.isNullOrEmpty(maternityDelivery)) {

                //store the full json
                parametersBuilder.addParameter("JSON_maternity_delivery", maternityDelivery);
            }
        }

        //assign the episode ward information as addition JSON.  Need to create JSON for this data
        String episodeStartWardCode = targetInpatientCds.getEpisodeStartWardCode();
        String episodeEndWardCode = targetInpatientCds.getEpisodeEndWardCode();
        if (!Strings.isNullOrEmpty(episodeStartWardCode) || !Strings.isNullOrEmpty(episodeEndWardCode)) {

            JsonObject episodeWardsObjs = new JsonObject();
            if (!Strings.isNullOrEmpty(episodeStartWardCode)) {

                episodeWardsObjs.addProperty("start_ward", episodeStartWardCode);
            }
            if (!Strings.isNullOrEmpty(episodeEndWardCode)) {

                episodeWardsObjs.addProperty("end_ward", episodeEndWardCode);
            }
            parametersBuilder.addParameter("JSON_wards", episodeWardsObjs.toString());
        }
    }
}