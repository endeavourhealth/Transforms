package org.endeavourhealth.transform.barts.cache;

import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.BasisTransformer;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.Patient;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.endeavourhealth.transform.common.BasisTransformer.createEncounterResourceId;
import static org.endeavourhealth.transform.common.BasisTransformer.getEncounterResourceId;

public class EncounterResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(EncounterResourceCache.class);

    private static Map<UUID, EncounterBuilder> encounterBuildersByUuid = new HashMap<>();
    private static Map<UUID, EncounterBuilder> deletedEncounterBuildersByUuid = new HashMap<>();


    public static void deleteEncounterBuilder(EncounterBuilder encounterBuilder) throws Exception {
        UUID key = UUID.fromString(encounterBuilder.getResourceId());
        LOG.trace("Deleting encounter " + key.toString());
        if (encounterBuildersByUuid.containsKey(key)) {
            encounterBuildersByUuid.remove(key);
        }
        deletedEncounterBuildersByUuid.put(key, encounterBuilder);
    }

    /*
    public static void deleteEncounterBuilder(String resourcId) throws Exception {
        UUID key = UUID.fromString(resourcId);
        deleteEncounterBuilder(key);
    }

    public static void deleteEncounterBuilder(UUID key) throws Exception {
        LOG.trace("Deleting encounter " + key.toString());
        if (encounterBuildersByUuid.containsKey(key)) {
            EncounterBuilder savedVersion = encounterBuildersByUuid.get(key);
            encounterBuildersByUuid.remove(key);
            deletedEncounterBuildersByUuid.put(key, savedVersion);
        }
    }
    */

    public static EncounterBuilder getEncounterBuilder(BartsCsvHelper csvHelper, String encounterId) throws Exception {

        ResourceId encounterResourceId = getEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, encounterId);

        if (encounterResourceId == null) {
            return null;
        }

        EncounterBuilder encounterBuilder = encounterBuildersByUuid.get(encounterResourceId.getResourceId());

        if (encounterBuilder == null) {

            Encounter encounter = (Encounter)csvHelper.retrieveResource(ResourceType.Encounter, encounterResourceId.getResourceId());
            if (encounter != null) {
                encounterBuilder = new EncounterBuilder(encounter);
                encounterBuildersByUuid.put(UUID.fromString(encounterBuilder.getResourceId()), encounterBuilder);
            }

        }

        return encounterBuilder;
    }

    public static EncounterBuilder createEncounterBuilder(CsvCell encounterIdCell) throws Exception {

        ResourceId encounterResourceId = createEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, encounterIdCell.getString());

        EncounterBuilder encounterBuilder = new EncounterBuilder();
        encounterBuilder.setId(encounterResourceId.getResourceId().toString(), encounterIdCell);

        encounterBuildersByUuid.put(encounterResourceId.getResourceId(), encounterBuilder);

        return encounterBuilder;
    }

    public static void fileEncounterResources(FhirResourceFiler fhirResourceFiler) throws Exception {

        LOG.trace("Saving " + encounterBuildersByUuid.size() + " encounters to the DB");

        for (UUID encounterId: encounterBuildersByUuid.keySet()) {
            EncounterBuilder EncounterBuilder = encounterBuildersByUuid.get(encounterId);
            BasisTransformer.savePatientResource(fhirResourceFiler, null, EncounterBuilder);
        }

        LOG.trace("Finishing saving " + encounterBuildersByUuid.size() + " encounters to the DB");

        LOG.trace("Deleting " + deletedEncounterBuildersByUuid.size() + " encounters from the DB");
        for (UUID encounterId: deletedEncounterBuildersByUuid.keySet()) {
            EncounterBuilder EncounterBuilder = deletedEncounterBuildersByUuid.get(encounterId);
            BasisTransformer.deletePatientResource(fhirResourceFiler, null, EncounterBuilder);
        }
        LOG.trace("Finishing deleting " + deletedEncounterBuildersByUuid.size() + " encounters from the DB");

        //clear down as everything has been saved
        encounterBuildersByUuid.clear();
    }
}
