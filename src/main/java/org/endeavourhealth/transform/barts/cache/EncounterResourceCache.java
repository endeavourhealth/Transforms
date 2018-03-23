package org.endeavourhealth.transform.barts.cache;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.BasisTransformer;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.EpisodeOfCare;
import org.hl7.fhir.instance.model.Identifier;
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

    public static EpisodeOfCareBuilder getEpisodeBuilder(BartsCsvHelper csvHelper, String episodeId) throws Exception {

        ResourceId episodeResourceId = getEpisodeOfCareResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, episodeId);

        if (episodeResourceId == null) {
            return null;
        }

        EpisodeOfCareBuilder episodeBuilder = episodeBuildersByUuid.get(episodeResourceId.getResourceId());

        if (episodeBuilder == null) {

            EpisodeOfCare episode = (EpisodeOfCare)csvHelper.retrieveResource(ResourceType.EpisodeOfCare, episodeResourceId.getResourceId());
            if (episode != null) {
                episodeBuilder = new EpisodeOfCareBuilder(episode);
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

    public static void saveNewEpisodeBuilderToCache(EpisodeOfCareBuilder episodeBuilder) throws Exception {
        UUID uuid = UUID.fromString(episodeBuilder.getResourceId());
        if (episodeBuildersByUuid.containsKey(uuid)) {
            episodeBuildersByUuid.replace(uuid, episodeBuilder);
        } else {
            episodeBuildersByUuid.put(uuid, episodeBuilder);
        }

    }

    public static EncounterBuilder createEncounterBuilder(CsvCell encounterIdCell, CsvCell finIdCell) throws Exception {

        ResourceId encounterResourceId = getEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, encounterIdCell.getString());

        if (encounterResourceId == null) {
            encounterResourceId = createEncounterResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, encounterIdCell.getString());
        }

        EncounterBuilder encounterBuilder = new EncounterBuilder();
        encounterBuilder.setId(encounterResourceId.getResourceId().toString(), encounterIdCell);

        IdentifierBuilder identifierBuilder = new IdentifierBuilder(encounterBuilder);
        identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_ENCOUNTER_ID);
        identifierBuilder.setUse(Identifier.IdentifierUse.OFFICIAL);
        identifierBuilder.setValue(encounterIdCell.getString(), encounterIdCell);

        if (finIdCell != null && !finIdCell.isEmpty()) {
            identifierBuilder = new IdentifierBuilder(encounterBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_FIN_EPISODE_ID);
            identifierBuilder.setValue(finIdCell.getString(), finIdCell);
        }

        encounterBuildersByUuid.put(encounterResourceId.getResourceId(), encounterBuilder);

        return encounterBuilder;
    }

    public static void fileEncounterResources(FhirResourceFiler fhirResourceFiler) throws Exception {

        LOG.trace("Saving " + episodeBuildersByUuid.size() + " Episodes to the DB");
        for (UUID episodeId: episodeBuildersByUuid.keySet()) {
            EpisodeOfCareBuilder episodeBuilder = episodeBuildersByUuid.get(episodeId);

            // Validation - to be removed later
            boolean error = false;
            EpisodeOfCare episodeOfCare = (EpisodeOfCare) episodeBuilder.getResource();

            if (episodeOfCare.hasStatus() == false) {
                LOG.error("Data error. Saving EoC without status.");
                error = true;
            }
            if (episodeOfCare.hasPeriod() == false || episodeOfCare.getPeriod().getStart() == null ) {
                LOG.error("Data error. Saving EoC without dates.");
                error = true;
            }
            if (episodeOfCare.getIdentifier() == null || episodeOfCare.getIdentifier().size() == 0) {
                LOG.error("Data error. Saving EoC without Identifiers.");
                error = true;
            }
            if (error) {
                LOG.error("Data error:" + FhirSerializationHelper.serializeResource(episodeOfCare));
            }

            BasisTransformer.savePatientResource(fhirResourceFiler, null, episodeBuilder);
        }
        LOG.trace("Finishing saving " + episodeBuildersByUuid.size() + " Episodes to the DB");

        LOG.trace("Saving " + encounterBuildersByUuid.size() + " encounters to the DB");
        for (UUID encounterId: encounterBuildersByUuid.keySet()) {
            EncounterBuilder EncounterBuilder = encounterBuildersByUuid.get(encounterId);

            // Validation - to be removed later
            boolean error = false;
            Encounter encounter = (Encounter) EncounterBuilder.getResource();

            if (encounter.hasStatus() == false) {
                LOG.error("Data error. Saving Encounter without status.");
                error = true;
            }
            if (encounter.hasPeriod() == false || encounter.getPeriod().getStart() == null ) {
                LOG.error("Data error. Saving Encounter without dates.");
                error = true;
            }
            if (encounter.getIdentifier() == null || encounter.getIdentifier().size() == 0) {
                LOG.error("Data error. Saving Encounter without Identifiers.");
                error = true;
            }
            if (encounter.hasEpisodeOfCare() == false || encounter.getEpisodeOfCare().size() == 0) {
                LOG.error("Data error. Saving Encounter without EoC reference.");
                error = true;
            }
            if (encounter.hasLocation() == false || encounter.getLocation().size() == 0) {
                LOG.error("Data error. Saving Encounter without location reference.");
                error = true;
            }
            if (error) {
                LOG.error("Data error:" + FhirSerializationHelper.serializeResource(encounter));
            }

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
        episodeBuildersByUuid.clear();
        encounterBuildersByUuid.clear();
        deletedEncounterBuildersByUuid.clear();
        //encounterDates.clear();
    }

}
