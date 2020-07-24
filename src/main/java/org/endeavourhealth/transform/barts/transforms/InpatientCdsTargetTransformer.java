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
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingInpatientCdsTarget;
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

import java.util.*;

public class InpatientCdsTargetTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(InpatientCdsTargetTransformer.class);
    private static Set<Long> patientInpatientEncounterDates = new HashSet<>();

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
//                Reference patientReference = parentEncounterBuilder.getPatient();
//                if (!parentEncounterBuilder.isIdMapped()) {
//                    patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
//                }
//                String patientUuid = ReferenceHelper.getReferenceId(patientReference);
                deleteHL7ReceiverPatientInpatientEncounters(targetInpatientCds, fhirResourceFiler, csvHelper);
                patientInpatientEncounterDates.clear();

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

        // Retrieve or create EpisodeOfCare for top level parent encounter only
        if (!isChildEncounter) {

            EpisodeOfCareBuilder episodeOfCareBuilder = csvHelper.getEpisodeOfCareCache().getEpisodeOfCareBuilder(targetInpatientCds);
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
        }
        Integer performerPersonnelId = targetInpatientCds.getPerformerPersonnelId();
        if (performerPersonnelId != null) {

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
            existingParentEncounterBuilder = new EncounterBuilder(existingParentEncounter);
        }
        existingParentEncounterList = new ContainedListBuilder(existingParentEncounterBuilder);

        EncounterBuilder admissionEncounterBuilder = null;
        EncounterBuilder dischargeEncounterBuilder = null;

        //episodeNumber = 01 then create the inpatient admission and the discharge encounters (if date set)
        if (episodeNumber.equalsIgnoreCase("01")) {

            admissionEncounterBuilder = new EncounterBuilder();
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
                dischargeEncounterBuilder = new EncounterBuilder();
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
                            = IdHelper.convertLocallyUniqueReferenceToEdsReference(childAdmissionRef, csvHelper);
                }

                existingParentEncounterList.addReference(childDischargeRef);
            }
        }

        //these are the 01, 02, 03, 04 subsequent episodes where activity happens, maternity, wards change etc.
        //also, critical care child encounters link back to these as their parents via their own transform
        EncounterBuilder episodeEncounterBuilder = new EncounterBuilder();
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

        //TODO: processing start and end wards?
//        ContainedParametersBuilder containedParametersBuilder
//                = new ContainedParametersBuilder(episodeEncounterBuilder);
//        containedParametersBuilder.removeContainedParameters();
//
//        String episodeStartWardCode = targetInpatientCds.getEpisodeStartWardCode();
//        if (!Strings.isNullOrEmpty(episodeStartWardCode)) {
//            containedParametersBuilder.addParameter("ip_episode_start_ward", "" + episodeStartWardCode);
//        }
//        String episodeEndWardCode = targetInpatientCds.getEpisodeEndWardCode();
//        if (!Strings.isNullOrEmpty(episodeEndWardCode)) {
//            containedParametersBuilder.addParameter("ip_episode_end_ward", "" + episodeEndWardCode);
//        }

        //TODO:  procedures associated with episode encounters - already in via specific transforms?
        // targetInpatientCds.getPrimaryProcedureOPCS());
        // targetInpatientCds.getSecondaryProcedureOPCS());
        // targetInpatientCds.getOtherProceduresOPCS());

        //TODO: mothers NHS number linking from birth records at subscriber
//        String maternityBirth = targetInpatientCds.getMaternityDataBirth();
//        //the encounter is about the baby and contains the mothers nhs number
//        if (!Strings.isNullOrEmpty(maternityBirth)) {
//            containedParametersBuilder.addParameter("maternity_birth", maternityBirth);
//        }
//
//        String maternityDelivery = targetInpatientCds.getMaternityDataDelivery();
//        //the encounter is about the mother and this is the birth(s) detail
//        if (!Strings.isNullOrEmpty(maternityDelivery)) {
//            containedParametersBuilder.addParameter("maternity_delivery", maternityBirth);
//        }

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

                            fhirResourceFiler.deletePatientResource(null, false, new EncounterBuilder(childEncounter));
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

        EncounterBuilder parentEncounterBuilder = new EncounterBuilder();
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
        Set<String> serviceIds = new HashSet<>();
        serviceIds.add(serviceUuid.toString());
        //get the list of patientId values for this service as Map<patientId, serviceId>
        Map<UUID, UUID> patientIdsForService = patientSearchDal.findPatientIdsForNhsNumber(serviceIds, nhsNumber);
        Set<UUID> patientIds = patientIdsForService.keySet();   //get the unique patientId values, >1 where >1 system

        //loop through all the patientIds for that patient to check the encounters
        for (UUID patientId: patientIds) {

            //LOG.debug("Checking patient: " + patientId.toString() + " for existing service: " + serviceUuid.toString() + " encounters");

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

                //LOG.debug("Existing HL7 Inpatient encounter " + existingEncounter.getId() + ", date: " + existingEncounter.getPeriod().getStart().toString() + ", cut off date: " + cutoff.toString());

                //if the HL7 Encounter is before our 12 hr cutoff, look to delete it
                if (existingEncounter.hasPeriod()
                        && existingEncounter.getPeriod().hasStart()
                        && existingEncounter.getPeriod().getStart().before(cutoff)) {

                    //finally, check it is an Inpatient encounter class before deleting
                    if (existingEncounter.getClass_().equals(Encounter.EncounterClass.INPATIENT)) {

                        LOG.debug("Checking existing Inpatient encounter date (long): " + existingEncounter.getPeriod().getStart().getTime() + " in dates array: " + patientInpatientEncounterDates.toArray());
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
                                EpisodeOfCare episodeOfCare
                                        = (EpisodeOfCare)resourceDal.getCurrentVersionAsResource(serviceUuid, ResourceType.EpisodeOfCare, episodeUuid);

                                if (episodeOfCare != null) {
                                    GenericBuilder builderEpisode = new GenericBuilder(episodeOfCare);
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
            MapResponse propertyResponse = IMClient.getMapProperty(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","inpatient",
                    "administrative_category_code", adminCategoryCode,"CM_NHS_DD"
            );
            MapResponse valueResponse = IMClient.getMapPropertyValue(valueRequest);

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
            MapResponse propertyResponse = IMClient.getMapProperty(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","inpatient",
                    "admission_method_code", admissionMethodCode,"CM_NHS_DD"
            );
            MapResponse valueResponse = IMClient.getMapPropertyValue(valueRequest);

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
            MapResponse propertyResponse = IMClient.getMapProperty(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","inpatient",
                    "admission_source_code", admissionSourceCode,"CM_NHS_DD"
            );
            MapResponse valueResponse = IMClient.getMapPropertyValue(valueRequest);

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
            MapResponse propertyResponse = IMClient.getMapProperty(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","inpatient",
                    "patient_classification", patientClassification,"CM_NHS_DD"
            );
            MapResponse valueResponse = IMClient.getMapPropertyValue(valueRequest);

            CodeableConcept ccValue = new CodeableConcept();
            ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                    .setSystem(valueResponse.getConcept().getScheme());
            parametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
        }

        String treatmentFunctionCode = targetInpatientCds.getTreatmentFunctionCode();
        if (!Strings.isNullOrEmpty(treatmentFunctionCode)) {

            MapColumnRequest propertyRequest = new MapColumnRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","inpatient",
                    "treatment_function_code"
            );
            MapResponse propertyResponse = IMClient.getMapProperty(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","inpatient",
                    "treatment_function_code", treatmentFunctionCode,"BartsCerner"
            );
            MapResponse valueResponse = IMClient.getMapPropertyValue(valueRequest);

            CodeableConcept ccValue = new CodeableConcept();
            ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                    .setSystem(valueResponse.getConcept().getScheme());
            parametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
        }

        //add the primary and secondary diagnosis codes as additional parameters.
        // TODO: Check these are filed as part of the diagnosis CDS transform as separate diagnosis records

//            String primaryDiagnosis = targetInpatientCds.getPrimaryDiagnosisICD();
//            if (!Strings.isNullOrEmpty(primaryDiagnosis)) {
//                containedParametersBuilderAdmission.addParameter("primary_diagnosis", primaryDiagnosis);
//            }
//            String secondaryDiagnosis = targetInpatientCds.getSecondaryDiagnosisICD();
//            if (!Strings.isNullOrEmpty(secondaryDiagnosis)) {
//                containedParametersBuilderAdmission.addParameter("secondary_diagnosis", secondaryDiagnosis);
//            }
//            String otherDiagnosis = targetInpatientCds.getOtherDiagnosisICD();
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
            MapResponse propertyResponse = IMClient.getMapProperty(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","inpatient",
                    "discharge_method", dischargeMethodCode,"CM_NHS_DD"
            );
            MapResponse valueResponse = IMClient.getMapPropertyValue(valueRequest);

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
            MapResponse propertyResponse = IMClient.getMapProperty(propertyRequest);

            MapColumnValueRequest valueRequest = new MapColumnValueRequest(
                    "CM_Org_Barts","CM_Sys_Cerner","CDS","inpatient",
                    "discharge_destination_code", dischargeDestinationCode,"CM_NHS_DD"
            );
            MapResponse valueResponse = IMClient.getMapPropertyValue(valueRequest);

            CodeableConcept ccValue = new CodeableConcept();
            ccValue.addCoding().setCode(valueResponse.getConcept().getCode())
                    .setSystem(valueResponse.getConcept().getScheme());
            parametersBuilder.addParameter(propertyResponse.getConcept().getCode(), ccValue);
        }
    }
}