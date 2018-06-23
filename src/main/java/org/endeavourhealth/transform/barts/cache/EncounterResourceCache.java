package org.endeavourhealth.transform.barts.cache;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.audit.QueuedMessageDalI;
import org.endeavourhealth.core.database.dal.audit.models.QueuedMessageType;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedListBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.GenericBuilder;
import org.endeavourhealth.transform.emis.csv.helpers.ReferenceList;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class EncounterResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(EncounterResourceCache.class);

    private Map<Long, EncounterBuilderProxy> encounterBuildersByEncounterId = new HashMap<>();
    private int countOffloaded = 0;
    private Set<Long> encounterIdsJustDeleted = new HashSet<>();
    private Map<Long, UUID> encountersWithChangedPatientUuids = new HashMap<>();

    /**
     * the ENCNT transformer deletes Encounters, and records that this has been done here,
     * so that the later transforms can check, since the deleted Encounter may not have reached the DB yet
     */
    public void deleteEncounter(EncounterBuilder encounterBuilder, CsvCell encounterIdCell, FhirResourceFiler fhirResourceFiler, CsvCurrentState parserState) throws Exception {

        //null may end up passed in, so just ignore
        if (encounterBuilder == null) {
            return;
        }

        //record that we know it's deleted
        Long encounterId = encounterIdCell.getLong();
        encounterIdsJustDeleted.add(encounterId);

        //remove from the cache
        encounterBuildersByEncounterId.remove(encounterId);

        boolean mapIds = !encounterBuilder.isIdMapped();
        fhirResourceFiler.deletePatientResource(parserState, mapIds, encounterBuilder);
    }




    public EncounterBuilder getEncounterBuilder(CsvCell encounterIdCell, CsvCell personIdCell, CsvCell activeIndicatorCell, BartsCsvHelper csvHelper) throws Exception {

        Long encounterId = encounterIdCell.getLong();

        //if we know we've deleted it, return null
        if (encounterIdsJustDeleted.contains(encounterId)) {
            return null;
        }

        EncounterBuilder encounterBuilder = null;
        EncounterBuilderProxy proxy = encounterBuildersByEncounterId.get(encounterId);
        if (proxy != null) {
            encounterBuilder = proxy.getEncounterBuilder(false);
        }

        if (encounterBuilder == null) {

            Encounter encounter = (Encounter)csvHelper.retrieveResourceForLocalId(ResourceType.Encounter, encounterId.toString());
            if (encounter != null) {
                encounterBuilder = new EncounterBuilder(encounter);

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

                //apply any newly linked child resources (observations, procedures etc.)
                ContainedListBuilder containedListBuilder = new ContainedListBuilder(encounterBuilder);
                ReferenceList newLinkedResources = csvHelper.getAndRemoveNewConsultationRelationships(encounterIdCell);
                if (newLinkedResources != null) {
                    for (int i=0; i<newLinkedResources.size(); i++) {
                        Reference reference = newLinkedResources.getReference(i);
                        CsvCell[] sourceCells = newLinkedResources.getSourceCells(i);
                        //note we need to convert the reference from a local ID one to a Discovery UUID one
                        reference = IdHelper.convertLocallyUniqueReferenceToEdsReference(reference, csvHelper);
                        containedListBuilder.addContainedListItem(reference, sourceCells);
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
                containedListBuilder.addReferences(newLinkedResources);
            }

            encounterBuildersByEncounterId.put(encounterId, new EncounterBuilderProxy(encounterBuilder));
            offloadEncountersIfNecessary();
        }

        return encounterBuilder;
    }

    /**
     * we have a max limit on the number of EncounterBuilders we can keep in memory, since keeping too many
     * will result in memory problems. So whenever the cache state changes, check
     */
    private void offloadEncountersIfNecessary() throws Exception {
        int cacheSize = encounterBuildersByEncounterId.size();
        cacheSize -= countOffloaded;

        int maxInMemory = TransformConfig.instance().getCernerEncounterCacheMaxSize();
        if (cacheSize > maxInMemory) {
            int toOffload = cacheSize - maxInMemory;

            for (Long key: encounterBuildersByEncounterId.keySet()) {
                EncounterBuilderProxy proxy = encounterBuildersByEncounterId.get(key);
                if (!proxy.isOffloaded()) {
                    proxy.offloadFromMemory();
                    toOffload --;
                    if (toOffload <= 0) {
                        break;
                    }
                }
            }
        }

    }

    public void fileEncounterResources(FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        //before saving the new ones work out any patients that we've changed
        Set<String> hsPatientUuidsChanged = new HashSet<>();

        LOG.trace("Saving " + encounterBuildersByEncounterId.size() + " encounters to the DB");
        for (Long encounterId: encounterBuildersByEncounterId.keySet()) {
            EncounterBuilderProxy proxy = encounterBuildersByEncounterId.get(encounterId);
            EncounterBuilder encounterBuilder = proxy.getEncounterBuilder(true);

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
        LOG.trace("Finishing saving " + encounterBuildersByEncounterId.size() + " encounters to the DB");

        //clear down as everything has been saved
        encounterBuildersByEncounterId.clear();
        encountersWithChangedPatientUuids.clear();
        encounterIdsJustDeleted.clear();

        //now delete any older HL7 Encounters for patients we've updated
        //but waiting until everything has been saved to the DB first
        fhirResourceFiler.waitUntilEverythingIsSaved();

        for (String patientUuid: hsPatientUuidsChanged) {
            deleteHl7ReceiverEncounters(UUID.fromString(patientUuid), fhirResourceFiler, csvHelper);
        }
    }

    private void deleteHl7ReceiverEncounters(UUID patientUuid, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        UUID serviceUuid = fhirResourceFiler.getServiceId();
        UUID systemUuid = fhirResourceFiler.getSystemId();

        //we want to delete any HL7 Encounter more than 24 hours older than the DW file extract date
        Date extractDateTime = csvHelper.getExtractDateTime();
        Date cutoff = new Date(extractDateTime.getTime() - (24 * 60 * 60 * 1000));

        ResourceDalI resourceDal = DalProvider.factoryResourceDal();
        List<ResourceWrapper> resourceWrappers = resourceDal.getResourcesByPatient(serviceUuid, systemUuid, patientUuid, ResourceType.Encounter.toString());
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
                fhirResourceFiler.deletePatientResource(null, false, new GenericBuilder(existingEncounter));
            }
        }
    }

    public UUID getOriginalPatientUuid(CsvCell encounterIdCell) {
        Long encounterId = encounterIdCell.getLong();
        return encountersWithChangedPatientUuids.get(encounterId);
    }

    /**
     * proxy class to hold an EncounterBuilder either in memory or offload it to the DB if we've got too many
     */
    class EncounterBuilderProxy {
        private EncounterBuilder encounterBuilder = null;
        private UUID tempStorageUuid = null;

        public EncounterBuilderProxy(EncounterBuilder encounterBuilder) {
            this.encounterBuilder = encounterBuilder;
        }

        /**
         * offloads the Encounter to the audit.queued_message table for safe keeping, to reduce memory load
         */
        public void offloadFromMemory() throws Exception {
            if (encounterBuilder == null) {
                return;
            }

            if (tempStorageUuid == null) {
                tempStorageUuid = UUID.randomUUID();
            }

            String json = FhirSerializationHelper.serializeResource(encounterBuilder.getResource());
            QueuedMessageDalI dal = DalProvider.factoryQueuedMessageDal();
            dal.save(tempStorageUuid, json, QueuedMessageType.ResourceTempStore);
LOG.debug("Offloaded " + encounterBuilder.getResourceId() + " to cache: " + this.tempStorageUuid);
            this.encounterBuilder = null;
            countOffloaded ++;
        }

        public boolean isOffloaded() {
            return this.encounterBuilder == null && this.tempStorageUuid != null;
        }

        /**
         * gets the Encounter, either from the variable or from the audit.queued_message table if it was offloaded
         */
        public EncounterBuilder getEncounterBuilder(boolean release) throws Exception {

            if (this.encounterBuilder == null && this.tempStorageUuid == null) {
                throw new Exception("Cannot get EncounterBuilder after releasing from proxy");
            }

            EncounterBuilder ret = null;
            if (encounterBuilder != null) {
                ret = encounterBuilder;
                if (release) {
                    this.encounterBuilder = null;
                }

            } else {
                QueuedMessageDalI dal = DalProvider.factoryQueuedMessageDal();
                String json = dal.getById(tempStorageUuid);
                Encounter encounter = (Encounter)FhirSerializationHelper.deserializeResource(json);
                ret = new EncounterBuilder(encounter);
                if (release) {
                    dal.delete(tempStorageUuid);
                    this.tempStorageUuid = null;
                } else {
                    countOffloaded --;
                }
LOG.debug("Restored " + ret.getResourceId() + " to cache: " + this.tempStorageUuid);
                //if we've just retrieved one from memory, we probably will need to write another one to DB
                offloadEncountersIfNecessary();
            }

            return ret;
        }
    }

}
