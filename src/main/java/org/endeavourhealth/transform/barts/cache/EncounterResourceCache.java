package org.endeavourhealth.transform.barts.cache;

import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.BasisTransformer;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.EpisodeOfCare;
import org.hl7.fhir.instance.model.Patient;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.endeavourhealth.transform.common.BasisTransformer.*;

public class EncounterResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(EncounterResourceCache.class);

    private static Map<UUID, EpisodeOfCareBuilder> episodeBuildersByUuid = new HashMap<>();
    private static Map<UUID, EncounterBuilder> encounterBuildersByUuid = new HashMap<>();
    private static Map<UUID, EncounterBuilder> deletedEncounterBuildersByUuid = new HashMap<>();
    private static Map<String, EncounterResourceCacheDateRecord> encounterDates = new HashMap<String, EncounterResourceCacheDateRecord>();


    public static void saveEncounterDates(String encounterId, CsvCell beginDateCell, CsvCell endDateCell) throws TransformException {
        EncounterResourceCacheDateRecord record = new EncounterResourceCacheDateRecord();

        record.setEncounterId(encounterId);

        if (beginDateCell != null) {
            record.setBeginDate(beginDateCell.getDate());
        }
        record.setBeginDateCell(beginDateCell);

        if (endDateCell != null) {
            record.setEndDate(endDateCell.getDate());
        }
        record.setEndDateCell(endDateCell);

        if (encounterDates.containsKey(record.getEncounterId())) {
            encounterDates.replace(record.getEncounterId(), record);
        } else {
            encounterDates.put(record.getEncounterId(), record);
        }
    }

    public static EncounterResourceCacheDateRecord getEncounterDates(String encounterId) {
        return encounterDates.get(encounterId);
    }

    public static void deleteEncounterBuilder(EncounterBuilder encounterBuilder) throws Exception {
        UUID key = UUID.fromString(encounterBuilder.getResourceId());
        LOG.trace("Deleting encounter " + key.toString());
        if (encounterBuildersByUuid.containsKey(key)) {
            encounterBuildersByUuid.remove(key);
        }
        deletedEncounterBuildersByUuid.put(key, encounterBuilder);
    }

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

    public static EpisodeOfCareBuilder getEpisodeBuilder(BartsCsvHelper csvHelper, String encounterId) throws Exception {

        ResourceId episodeResourceId = getEpisodeOfCareResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, encounterId);

        if (episodeResourceId == null) {
            return null;
        }

        EpisodeOfCareBuilder episodeBuilder = episodeBuildersByUuid.get(episodeResourceId.getResourceId());

        if (episodeBuilder == null) {

            EpisodeOfCare encounter = (EpisodeOfCare)csvHelper.retrieveResource(ResourceType.EpisodeOfCare, episodeResourceId.getResourceId());
            if (encounter != null) {
                episodeBuilder = new EpisodeOfCareBuilder(encounter);
                episodeBuildersByUuid.put(UUID.fromString(episodeBuilder.getResourceId()), episodeBuilder);
            }

        }

        return episodeBuilder;
    }

    public static EpisodeOfCareBuilder createEpisodeBuilder(CsvCell episodeIdCell) throws Exception {

        ResourceId episodeResourceId = getEpisodeOfCareResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, episodeIdCell.getString());

        if (episodeResourceId == null) {
            episodeResourceId = createEpisodeOfCareResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, episodeIdCell.getString());
        }

        EpisodeOfCareBuilder episodeBuilder = new EpisodeOfCareBuilder();
        episodeBuilder.setId(episodeResourceId.getResourceId().toString(), episodeIdCell);

        episodeBuildersByUuid.put(episodeResourceId.getResourceId(), episodeBuilder);

        return episodeBuilder;
    }

    public static EncounterBuilder createEncounterBuilder(CsvCell encounterIdCell) throws Exception {

        ResourceId encounterResourceId = getEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, encounterIdCell.getString());

        if (encounterResourceId == null) {
            encounterResourceId = createEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, encounterIdCell.getString());
        }

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
        deletedEncounterBuildersByUuid.clear();
        encounterDates.clear();
    }
}
