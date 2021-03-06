package org.endeavourhealth.transform.barts.cache;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.referenceLists.ReferenceList;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedListBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.GenericBuilder;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class EncounterResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(EncounterResourceCache.class);

    public static final String DUPLICATE_EMERGENCCY_ENCOUNTER_SUFFIX = ":EmergencyDuplicate";

    private final BartsCsvHelper csvHelper;
    private ResourceCache<String, EncounterBuilder> encounterBuildersByEncounterId = new ResourceCache<>();
    private Set<String> encounterIdsJustDeleted = new HashSet<>();
    private Map<String, UUID> encountersWithChangedPatientUuids = new HashMap<>();

    public EncounterResourceCache(BartsCsvHelper csvHelper) {
        this.csvHelper = csvHelper;
    }


    /**
     * the ENCNT transformer deletes Encounters, and records that this has been done here,
     * so that the later transforms can check, since the deleted Encounter may not have reached the DB yet
     */
    public void deleteEncounter(EncounterBuilder encounterBuilder, CsvCell encounterIdCell, CsvCell personIdCell, FhirResourceFiler fhirResourceFiler, CsvCurrentState parserState, CsvCell triggeringCell) throws Exception {

        //null may end up passed in, so just ignore
        if (encounterBuilder == null) {
            return;
        }

        //record that we know it's deleted
        String encounterId = encounterIdCell.getString();
        encounterIdsJustDeleted.add(encounterId);

        //remove from the cache
        //not necessary, since the builder is passed in, it means it will already have been removed
        //encounterBuildersByEncounterId.removeFromCache(encounterId);

        boolean mapIds = !encounterBuilder.isIdMapped();
        encounterBuilder.setDeletedAudit(triggeringCell);
        fhirResourceFiler.deletePatientResource(parserState, mapIds, encounterBuilder);

        //we create two FHIR Encounters for Cerner ENCNT records where an A&E attendance turns into
        //an admission, so we need to make sure that we delete BOTH
        EncounterBuilder duplicateEmergencyEncounterBuilder = borrowDuplicateEmergencyEncounterBuilder(encounterIdCell, personIdCell);
        if (duplicateEmergencyEncounterBuilder != null) {
            String duplicateEncounterId = encounterIdCell.getString() + DUPLICATE_EMERGENCCY_ENCOUNTER_SUFFIX;
            encounterIdsJustDeleted.add(duplicateEncounterId);

            boolean mapDuplicateEncounterIds = !duplicateEmergencyEncounterBuilder.isIdMapped();
            duplicateEmergencyEncounterBuilder.setDeletedAudit(triggeringCell);
            fhirResourceFiler.deletePatientResource(parserState, mapDuplicateEncounterIds, duplicateEmergencyEncounterBuilder);
        }
    }

    public void returnEncounterBuilder(CsvCell encounterIdCell, EncounterBuilder encounterBuilder) throws Exception {
        String encounterId = encounterIdCell.getString();
        encounterBuildersByEncounterId.addToCache(encounterId, encounterBuilder);
    }

    public void returnDuplicateEmergencyEncounterBuilder(CsvCell encounterIdCell, EncounterBuilder encounterBuilder) throws Exception {
        String encounterId = encounterIdCell.getString() + DUPLICATE_EMERGENCCY_ENCOUNTER_SUFFIX;
        encounterBuildersByEncounterId.addToCache(encounterId, encounterBuilder);
    }

    /**
     * note that the caching used by this function means that any encounter GOT must also be RETURNED when it's finished with,
     * otherwise it won't be saved
     */
    public EncounterBuilder borrowEncounterBuilder(CsvCell encounterIdCell, CsvCell personIdCell, CsvCell activeIndicatorCell) throws Exception {
        String encounterId = encounterIdCell.getString();

        //if we know we've deleted it, return null
        if (encounterIdsJustDeleted.contains(encounterId)) {
            return null;
        }

        //check the cache
        EncounterBuilder cachedResource = encounterBuildersByEncounterId.getAndRemoveFromCache(encounterId);
        if (cachedResource != null) {
            return cachedResource;
        }

        EncounterBuilder encounterBuilder = null;

        //if not in the cache, check the DB
        Encounter encounter = (Encounter)csvHelper.retrieveResourceForLocalId(ResourceType.Encounter, encounterId);
        if (encounter != null) {
            encounterBuilder = new EncounterBuilder(encounter);

            //always set the person ID fresh, in case the record has been moved to another patient, remembering to forward map to a UUID
            //but track the old patient UUID so we can use it to update dependent resources
            if (personIdCell != null
                    && !BartsCsvHelper.isEmptyOrIsZero(personIdCell)) { //for deleted ENCNT records, we don't get a personID

                Reference oldPatientReference = encounter.getPatient();
                UUID oldPatientUuid = UUID.fromString(ReferenceHelper.getReferenceId(oldPatientReference));
                UUID currentPatientUuid = IdHelper.getOrCreateEdsResourceId(csvHelper.getServiceId(), ResourceType.Patient, personIdCell.getString());
                if (!oldPatientUuid.equals(currentPatientUuid)) {

                    encountersWithChangedPatientUuids.put(encounterId, oldPatientUuid);

                    Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, currentPatientUuid.toString());
                    encounterBuilder.setPatient(patientReference, personIdCell);
                }
            }

            //apply any newly linked child resources (observations, procedures etc.)
            ContainedListBuilder containedListBuilder = new ContainedListBuilder(encounterBuilder);
            ReferenceList newLinkedResources = csvHelper.getAndRemoveNewConsultationRelationships(encounterIdCell);
            if (newLinkedResources != null) {
                //LOG.debug("Encounter " + encounterId + " has " + newLinkedResources.size() + " child resources");
                for (int i=0; i<newLinkedResources.size(); i++) {
                    Reference reference = newLinkedResources.getReference(i);
                    CsvCell[] sourceCells = newLinkedResources.getSourceCells(i);
                    //note we need to convert the reference from a local ID one to a Discovery UUID one
                    reference = IdHelper.convertLocallyUniqueReferenceToEdsReference(reference, csvHelper);
                    containedListBuilder.addReference(reference, sourceCells);
                }
            }


        } else {

            //if our CSV record is non-active, it means it's deleted, so don't create a builder
            if (!activeIndicatorCell.getIntAsBoolean()) {
                encounterIdsJustDeleted.add(encounterId);
                return null;
            }

            encounterBuilder = new EncounterBuilder();
            encounterBuilder.setId(encounterIdCell.getString(), encounterIdCell);

            //set the patient reference
            if (personIdCell != null) {
                Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, personIdCell.getString());
                encounterBuilder.setPatient(patientReference, personIdCell);
            } else {
                //if we've not been given a person ID we can't create the EncounterBuilder
                return null;
            }

            //apply any newly linked child resources (observations, procedures etc.)
            ContainedListBuilder containedListBuilder = new ContainedListBuilder(encounterBuilder);
            ReferenceList newLinkedResources = csvHelper.getAndRemoveNewConsultationRelationships(encounterIdCell);
            if (newLinkedResources != null) {
                //LOG.debug("Encounter " + encounterId + " has " + newLinkedResources.size() + " child resources");
                containedListBuilder.addReferences(newLinkedResources);
            }
        }

        return encounterBuilder;
    }

    /**
     * looks for an Encounter resource mapped to from the Cerner Encounter ID with the special suffix used
     * when we duplicate an encounter to keep a record of A&E attendances. Unlike the other "borrow..." fn,
     * this one DOES NOT create new encounters
     */
    public EncounterBuilder borrowDuplicateEmergencyEncounterBuilder(CsvCell encounterIdCell, CsvCell personIdCell) throws Exception {
        String encounterId = encounterIdCell.getString() + DUPLICATE_EMERGENCCY_ENCOUNTER_SUFFIX;

        //if we know we've deleted it, return null
        if (encounterIdsJustDeleted.contains(encounterId)) {
            return null;
        }

        //check the cache
        EncounterBuilder cachedResource = encounterBuildersByEncounterId.getAndRemoveFromCache(encounterId);
        if (cachedResource != null) {
            return cachedResource;
        }

        //if not in the cache, check the DB
        Encounter encounter = (Encounter)csvHelper.retrieveResourceForLocalId(ResourceType.Encounter, encounterId);
        if (encounter == null) {
            return null;
        }

        EncounterBuilder encounterBuilder = new EncounterBuilder(encounter);

        //always set the person ID fresh, in case the record has been moved to another patient, remembering to forward map to a UUID
        //but track the old patient UUID so we can use it to update dependent resources
        if (personIdCell != null
                && !BartsCsvHelper.isEmptyOrIsZero(personIdCell)) { //for deleted ENCNT records, we don't get a personID

            Reference oldPatientReference = encounter.getPatient();
            UUID oldPatientUuid = UUID.fromString(ReferenceHelper.getReferenceId(oldPatientReference));
            UUID currentPatientUuid = IdHelper.getEdsResourceId(csvHelper.getServiceId(), ResourceType.Patient, personIdCell.getString());
            if (!oldPatientUuid.equals(currentPatientUuid)) {

                encountersWithChangedPatientUuids.put(encounterId, oldPatientUuid);

                Reference patientReference = ReferenceHelper.createReference(ResourceType.Patient, currentPatientUuid.toString());
                encounterBuilder.setPatient(patientReference, personIdCell);
            }
        }

        return encounterBuilder;
    }


    public void fileEncounterResources(FhirResourceFiler fhirResourceFiler) throws Exception {

        //before saving the new ones work out any patients that we've changed
        Set<String> hsPatientUuidsChanged = new HashSet<>();

        LOG.trace("Saving " + encounterBuildersByEncounterId.size() + " encounters to the DB");
        for (String encounterId: encounterBuildersByEncounterId.keySet()) {
            EncounterBuilder encounterBuilder = encounterBuildersByEncounterId.getAndRemoveFromCache(encounterId);

            //find the patient UUID for the encounter, so we can tidy up HL7 encounters after doing all the saving
            Reference patientReference = encounterBuilder.getPatient();
            if (!encounterBuilder.isIdMapped()) {
                patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
            }
            String patientUuid = ReferenceHelper.getReferenceId(patientReference);
            hsPatientUuidsChanged.add(patientUuid);

            //and save the resource
            boolean mapIds = !encounterBuilder.isIdMapped();
            fhirResourceFiler.savePatientResource(null, mapIds, encounterBuilder);
        }
        LOG.trace("Finishing saving encounters to the DB");

        //clear down as everything has been saved
        encounterBuildersByEncounterId.clear();
        encountersWithChangedPatientUuids.clear();
        encounterIdsJustDeleted.clear();

        //now delete any older HL7 Encounters for patients we've updated
        //but waiting until everything has been saved to the DB first
        fhirResourceFiler.waitUntilEverythingIsSaved();

        for (String patientUuid: hsPatientUuidsChanged) {
            deleteHl7ReceiverEncounters(UUID.fromString(patientUuid), fhirResourceFiler);
        }
    }

    private void deleteHl7ReceiverEncounters(UUID patientUuid, FhirResourceFiler fhirResourceFiler) throws Exception {

        UUID serviceUuid = fhirResourceFiler.getServiceId();
        UUID systemUuid = fhirResourceFiler.getSystemId();

        //we want to delete any HL7 Encounter more than 24 hours older than the DW file extract date
        Date extractDateTime = csvHelper.getExtractDateTime();
        Date cutoff = new Date(extractDateTime.getTime() - (24 * 60 * 60 * 1000));

        ResourceDalI resourceDal = DalProvider.factoryResourceDal();
        List<ResourceWrapper> resourceWrappers = resourceDal.getResourcesByPatient(serviceUuid, patientUuid, ResourceType.Encounter.toString());
        for (ResourceWrapper wrapper: resourceWrappers) {

            //if this episode is for our own system ID (i.e. DW feed), then leave it
            UUID wrapperSystemId = wrapper.getSystemId();
            if (wrapperSystemId.equals(systemUuid)) {
                continue;
            }

            String json = wrapper.getResourceData();
            Encounter existingEncounter = (Encounter)FhirSerializationHelper.deserializeResource(json);

            //if the HL7 Encounter has no date info at all or is before our cutoff, delete it
            if (!existingEncounter.hasPeriod()
                    || !existingEncounter.getPeriod().hasStart()
                    || existingEncounter.getPeriod().getStart().before(cutoff)) {
                GenericBuilder builder = new GenericBuilder(existingEncounter);
                //we have no audit for deleting these encounters, since it's not triggered by a specific piece of data
                //builder.setDeletedAudit(...);
                fhirResourceFiler.deletePatientResource(null, false, builder);
            }
        }
    }

    public UUID getOriginalPatientUuid(CsvCell encounterIdCell) {
        String encounterId = encounterIdCell.getString();
        return encountersWithChangedPatientUuids.get(encounterId);
    }


    /**
     * if we have had an error that's caused us to drop out of the transform, we can call this to tidy up
     * anything we've saved to the audit.queued_message table
     */
    public void cleanUpResourceCache() {
        try {
            encounterBuildersByEncounterId.clear();
        } catch (Exception ex) {
            LOG.error("Error cleaning up cache", ex);
        }
    }

}
