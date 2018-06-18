package org.endeavourhealth.transform.barts.cache;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.hl7.fhir.instance.model.EpisodeOfCare;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class EpisodeOfCareResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(EpisodeOfCareResourceCache.class);

    private static Map<String, EpisodeOfCareBuilder> episodeBuildersByEpisodeId = new HashMap<>();

    /**
     * retrieves or creates an EpisodeOfCareBuilder, using one of three identifiers
     */
    public static EpisodeOfCareBuilder getEpisodeOfCareBuilder(CsvCell episodeIdentiferCell,
                                                               CsvCell encounterIdCell,
                                                               CsvCell personIdCell,
                                                               CsvCell activeIndicatorCell,
                                                               BartsCsvHelper csvHelper) throws Exception {

        //we want to create an EpisodeOfCare as early as possible, but don't always have an episode ID that
        //early. If we don't have an episode ID, then we don't create an episodeOfCare
        if (!BartsCsvHelper.isEmptyOrIsZero(episodeIdentiferCell)) {
            String episodeId = episodeIdentiferCell.getString();

            //make sure a UUID exists for the EpisodeOfCare because the reference on Encounters will be set
            //before the Episode gets ID mapped. So do this to force the Episode ID -> UUID mapping is saved now
            IdHelper.getOrCreateEdsResourceId(csvHelper.getServiceId(), ResourceType.EpisodeOfCare, episodeId, UUID.randomUUID());

            //save a mapping of Encounter ID to episode ID so that we can later look up the episode ID just from Encounter ID
            String encounterId = encounterIdCell.getString();
            if (csvHelper.getInternalId(InternalIdMap.TYPE_ENCOUNTER_ID_TO_EPISODE_UUID, encounterId) == null) {
                csvHelper.saveInternalId(InternalIdMap.TYPE_ENCOUNTER_ID_TO_EPISODE_UUID, encounterId, episodeId);
            }

            //retrieve our resource and return
            return retrieveAndCacheBuilder(episodeId, personIdCell, csvHelper, activeIndicatorCell);
        }

        //if we don't have an Episode ID, then just try looking up by Encounter ID using the mapping created above
        String encounterId = encounterIdCell.getString();
        String episodeId = csvHelper.getInternalId(InternalIdMap.TYPE_ENCOUNTER_ID_TO_EPISODE_UUID, encounterId);
        if (!Strings.isNullOrEmpty(episodeId)) {

            return retrieveAndCacheBuilder(episodeId, personIdCell, csvHelper, activeIndicatorCell);
        }

        return null;
    }

    private static EpisodeOfCareBuilder retrieveAndCacheBuilder(String episodeId, CsvCell personIdCell, BartsCsvHelper csvHelper, CsvCell activeIndicatorCell) throws Exception {

        EpisodeOfCareBuilder builder = episodeBuildersByEpisodeId.get(episodeId);
        if (builder == null) {

            EpisodeOfCare episodeOfCare = (EpisodeOfCare)csvHelper.retrieveResourceForLocalId(ResourceType.EpisodeOfCare, episodeId);
            if (episodeOfCare != null) {
                builder = new EpisodeOfCareBuilder(episodeOfCare);

                //always update the patient reference, in case we've merged records, remembering to map from local ID to UUID since this has already been ID mapped
                Reference patientReference = csvHelper.createPatientReference(personIdCell);
                patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, csvHelper);
                builder.setPatient(patientReference, personIdCell);

            } else {
                //if we're deleting something, don't start creating a new episodeOfCare
                if (!activeIndicatorCell.getIntAsBoolean()) {
                    return null;
                }

                builder = new EpisodeOfCareBuilder();
                builder.setId(episodeId);

                String orgId = csvHelper.findOrgRefIdForBarts();
                Reference orgReference = ReferenceHelper.createReference(ResourceType.Organization, orgId);
                builder.setManagingOrganisation(orgReference);

                Reference patientReference = csvHelper.createPatientReference(personIdCell);
                builder.setPatient(patientReference, personIdCell);
            }

            episodeBuildersByEpisodeId.put(episodeId, builder);
        }

        return builder;
    }

    /*
    * Set unknown values to null
    * For non-AE encounters set 'aeArrivalDateTime' to null
     */
    /*public static EpisodeOfCareBuilder readOrCreateEpisodeOfCareBuilder(CsvCell episodeIdentiferCell, CsvCell finIdCell, CsvCell encounterIdCell, CsvCell personIdCell, UUID personUUID, BartsCsvHelper csvHelper, ParserI parser) throws Exception {
        boolean newEoCCreated = false;
        String FINalternateEpisodeUUID = null;
        String encounterAlternateEpisodeUUID = null;
        EpisodeOfCareBuilder episodeOfCareBuilder = null;

        String episodeIdentiferCellString = null;
        if (episodeIdentiferCell != null && episodeIdentiferCell.getString().trim().length() > 0) {
            episodeIdentiferCellString = episodeIdentiferCell.getString().trim();
        }
        String finIdCellString = null;
        if (finIdCell != null && finIdCell.getString().trim().length() > 0) {
            finIdCellString = finIdCell.getString().trim();
        }
        String encounterIdCellString = null;
        if (encounterIdCell != null && encounterIdCell.getString().trim().length() > 0) {
            encounterIdCellString = encounterIdCell.getString().trim();
        }
        LOG.debug("Search for episodeOfCareBuilder. EpisodeId=" + episodeIdentiferCellString + " FINNo="  + finIdCellString + " EncounterId=" + encounterIdCellString);


        if (episodeIdentiferCellString != null) {
            episodeOfCareBuilder = getEpisodeBuilder(csvHelper, episodeIdentiferCellString);
            if (episodeOfCareBuilder == null) {
                LOG.debug("episodeOfCareBuilder not found for id:" + episodeIdentiferCellString);
            }

            if (episodeOfCareBuilder == null && finIdCellString != null) {
                // EoC not found using Episode-id - try FIN-no (if it was created before it got the episode id)
                FINalternateEpisodeUUID = csvHelper.getInternalId(InternalIdMap.TYPE_FIN_NO_TO_EPISODE_UUID, finIdCell.getString());
                if (FINalternateEpisodeUUID != null) {
                    episodeOfCareBuilder = getEpisodeBuilder(csvHelper, UUID.fromString(FINalternateEpisodeUUID));
                    if (episodeOfCareBuilder != null) {
                        // Save the resource UUID so it can be found using episode-id next time
                        try {
                            createEpisodeOfCareResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, episodeIdentiferCellString, UUID.fromString(FINalternateEpisodeUUID));
                        }
                        catch (Exception ex) {
                            if (ex.getMessage().indexOf("duplicate key") > 0 && ex.getMessage().indexOf("duplicate key") > 0) {
                                TransformWarnings.log(LOG, parser, "FINNo {} previously belonged to different EpisodeOfCare({}). Record {} in file {}", finIdCellString, FINalternateEpisodeUUID, parser.getCurrentState().getRecordNumber(), parser.getFilePath());
                            } else {
                                throw ex;
                            }
                        }
                    }
                }
            }
            if (episodeOfCareBuilder == null) {
                // EoC not found using Episode-id or FIN-no - try Encounter-id (if it was created before it got the episode id and FIN no)
                encounterAlternateEpisodeUUID = csvHelper.getInternalId(InternalIdMap.TYPE_ENCOUNTER_ID_TO_EPISODE_UUID, encounterIdCellString);
                if (encounterAlternateEpisodeUUID != null) {
                    episodeOfCareBuilder = EncounterResourceCache.getEpisodeBuilder(csvHelper, UUID.fromString(encounterAlternateEpisodeUUID));
                    if (episodeOfCareBuilder != null) {
                        // Save the resource UUID so it can be found using episode-id next time
                        try {
                            createEpisodeOfCareResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, episodeIdentiferCellString, UUID.fromString(encounterAlternateEpisodeUUID));
                        }
                        catch (Exception ex) {
                            if (ex.getMessage().indexOf("duplicate key") > 0 && ex.getMessage().indexOf("duplicate key") > 0) {
                                TransformWarnings.log(LOG, parser, "EncounterId {} previously belonged to different EpisodeOfCare({}). Record {} in file {}", encounterIdCellString, encounterAlternateEpisodeUUID, parser.getCurrentState().getRecordNumber(), parser.getFilePath());
                            } else {
                                throw ex;
                            }
                        }
                    }
                }
            }
            if (episodeOfCareBuilder == null) {
                // EoC not  found - create new using episode-id
                ResourceId resourceId = getEpisodeOfCareResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, episodeIdentiferCellString);
                if (resourceId == null) {
                    resourceId = createEpisodeOfCareResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, episodeIdentiferCellString);
                }
                episodeOfCareBuilder = createNewEpisodeOfCareBuilder(episodeIdentiferCell, personIdCell, personUUID, null, null);
                episodeOfCareBuilder.setId(resourceId.getResourceId().toString(), episodeIdentiferCell);
                saveNewEpisodeBuilderToCache(episodeOfCareBuilder);
                newEoCCreated = true;
            }

        } else if (finIdCellString != null) {
            LOG.debug("Search using FINNo");
            // Episode-id not present - use FIN NO

            FINalternateEpisodeUUID = csvHelper.getInternalId(InternalIdMap.TYPE_FIN_NO_TO_EPISODE_UUID, finIdCellString);
            if (FINalternateEpisodeUUID == null) {
                // Check if encounter was previously saved using encounter id
                encounterAlternateEpisodeUUID = csvHelper.getInternalId(InternalIdMap.TYPE_ENCOUNTER_ID_TO_EPISODE_UUID, encounterIdCellString);
                if (encounterAlternateEpisodeUUID != null) {
                    episodeOfCareBuilder = EncounterResourceCache.getEpisodeBuilder(csvHelper, UUID.fromString(encounterAlternateEpisodeUUID));
                }

                if(episodeOfCareBuilder == null) {
                    episodeOfCareBuilder = createNewEpisodeOfCareBuilder(episodeIdentiferCell, personIdCell, personUUID, finIdCell, null);
                    episodeOfCareBuilder.setId(UUID.randomUUID().toString(), finIdCell);
                    EncounterResourceCache.saveNewEpisodeBuilderToCache(episodeOfCareBuilder);
                    newEoCCreated = true;
                }

            } else {
                episodeOfCareBuilder = EncounterResourceCache.getEpisodeBuilder(csvHelper, UUID.fromString(FINalternateEpisodeUUID));
                if (episodeOfCareBuilder == null) {
                    episodeOfCareBuilder = createNewEpisodeOfCareBuilder(episodeIdentiferCell, personIdCell, personUUID, finIdCell, null);
                    episodeOfCareBuilder.setId(FINalternateEpisodeUUID, finIdCell);
                    EncounterResourceCache.saveNewEpisodeBuilderToCache(episodeOfCareBuilder);
                    newEoCCreated = true;
                }
            }

        } else {
            LOG.debug("Search using EncounterId");
            // Neither Episode-id nor FIN No present - use encounter-id
            encounterAlternateEpisodeUUID = csvHelper.getInternalId(InternalIdMap.TYPE_ENCOUNTER_ID_TO_EPISODE_UUID, encounterIdCellString);
            if (encounterAlternateEpisodeUUID == null) {
                episodeOfCareBuilder = createNewEpisodeOfCareBuilder(episodeIdentiferCell, personIdCell, personUUID, null, encounterIdCell);
                episodeOfCareBuilder.setId(UUID.randomUUID().toString(), episodeIdentiferCell);
                EncounterResourceCache.saveNewEpisodeBuilderToCache(episodeOfCareBuilder);
                newEoCCreated = true;
            } else {
                episodeOfCareBuilder = EncounterResourceCache.getEpisodeBuilder(csvHelper, UUID.fromString(encounterAlternateEpisodeUUID));
                if (episodeOfCareBuilder == null) {
                    episodeOfCareBuilder = createNewEpisodeOfCareBuilder(episodeIdentiferCell, personIdCell, personUUID, null, encounterIdCell);
                    episodeOfCareBuilder.setId(encounterAlternateEpisodeUUID, encounterIdCell);
                    EncounterResourceCache.saveNewEpisodeBuilderToCache(episodeOfCareBuilder);
                    newEoCCreated = true;
                }
            }
        }

        if (newEoCCreated) {
            LOG.debug("Create new episodeOfCareBuilder " + episodeOfCareBuilder.getResourceId() + " EpisodeId=" + episodeIdentiferCellString + " FINNo="  + finIdCellString + " EncounterId=" + encounterIdCellString);
        }
        if (finIdCellString != null) {
            csvHelper.saveInternalId(InternalIdMap.TYPE_FIN_NO_TO_EPISODE_UUID, finIdCellString, episodeOfCareBuilder.getResourceId());
        }
        csvHelper.saveInternalId(InternalIdMap.TYPE_ENCOUNTER_ID_TO_EPISODE_UUID, encounterIdCellString, episodeOfCareBuilder.getResourceId());

        return episodeOfCareBuilder;
    }*/

    /*private static EpisodeOfCareBuilder createNewEpisodeOfCareBuilder(CsvCell episodeIdentiferCell, CsvCell personIdCell, UUID personUUID, CsvCell finIdCell, CsvCell encounterIdCell) {
        EpisodeOfCareBuilder episodeOfCareBuilder = new EpisodeOfCareBuilder();

        episodeOfCareBuilder.setStatus(EpisodeOfCare.EpisodeOfCareStatus.ACTIVE);

        if (episodeIdentiferCell != null && !episodeIdentiferCell.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(episodeOfCareBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.OFFICIAL);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_EPISODE_ID);
            identifierBuilder.setValue(episodeIdentiferCell.getString(), episodeIdentiferCell);
        }

        if (finIdCell != null && !finIdCell.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(episodeOfCareBuilder);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_FIN_EPISODE_ID);
            identifierBuilder.setUse(Identifier.IdentifierUse.TEMP);
            identifierBuilder.setValue(finIdCell.getString(), finIdCell);
        }

        if (encounterIdCell != null && !encounterIdCell.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(episodeOfCareBuilder);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_ENCOUNTER_ID);
            identifierBuilder.setUse(Identifier.IdentifierUse.TEMP);
            identifierBuilder.setValue(encounterIdCell.getString(), encounterIdCell);
        }

        if (personIdCell != null && !personIdCell.isEmpty()) {
            episodeOfCareBuilder.setPatient(ReferenceHelper.createReference(ResourceType.Patient, personUUID.toString()), personIdCell);
        }

        return episodeOfCareBuilder;
    }*/


    /*private static EpisodeOfCareBuilder getEpisodeBuilder(BartsCsvHelper csvHelper, String episodeId) throws Exception {

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
    }*/

    /*private static EpisodeOfCareBuilder getEpisodeBuilder(BartsCsvHelper csvHelper, UUID resourceId) throws Exception {

        EpisodeOfCareBuilder episodeBuilder = episodeBuildersByUuid.get(resourceId);

        if (episodeBuilder == null) {

            EpisodeOfCare episode = (EpisodeOfCare)csvHelper.retrieveResource(ResourceType.EpisodeOfCare, resourceId);
            if (episode != null) {
                episodeBuilder = new EpisodeOfCareBuilder(episode);
                episodeBuildersByUuid.put(UUID.fromString(episodeBuilder.getResourceId()), episodeBuilder);
            }

        }

        return episodeBuilder;
    }*/

    /*private static EpisodeOfCareBuilder createEpisodeBuilder(CsvCell episodeIdCell) throws Exception {

        ResourceId episodeResourceId = getEpisodeOfCareResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, episodeIdCell.getString());

        if (episodeResourceId == null) {
            episodeResourceId = createEpisodeOfCareResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, episodeIdCell.getString());
        }

        EpisodeOfCareBuilder episodeBuilder = new EpisodeOfCareBuilder();
        episodeBuilder.setId(episodeResourceId.getResourceId().toString(), episodeIdCell);

        episodeBuildersByUuid.put(episodeResourceId.getResourceId(), episodeBuilder);

        return episodeBuilder;
    }*/

    /*private static void saveNewEpisodeBuilderToCache(EpisodeOfCareBuilder episodeBuilder) throws Exception {
        UUID uuid = UUID.fromString(episodeBuilder.getResourceId());
        if (episodeBuildersByUuid.containsKey(uuid)) {
            episodeBuildersByUuid.replace(uuid, episodeBuilder);
        } else {
            episodeBuildersByUuid.put(uuid, episodeBuilder);
        }

    }*/

    public static void fileResources(FhirResourceFiler fhirResourceFiler) throws Exception {

        LOG.trace("Saving " + episodeBuildersByEpisodeId.size() + " Episodes to the DB");
        for (String episodeId: episodeBuildersByEpisodeId.keySet()) {
            EpisodeOfCareBuilder episodeBuilder = episodeBuildersByEpisodeId.get(episodeId);

            // Validation - to be removed later
            boolean error = false;
            EpisodeOfCare episodeOfCare = (EpisodeOfCare) episodeBuilder.getResource();

            if (!episodeOfCare.hasStatus()) {
                LOG.error("Data error. Saving EoC without status.");
                error = true;
            }
            if (!episodeOfCare.hasPeriod() || episodeOfCare.getPeriod().getStart() == null ) {
                LOG.error("Data error. Saving EoC without dates.");
                error = true;
            }
            if (episodeOfCare.getIdentifier() == null || episodeOfCare.getIdentifier().size() == 0) {
                LOG.error("Data error. Saving EoC without Identifiers.");
                error = true;
            }
            if (error) {
                throw new TransformException("Data error:" + FhirSerializationHelper.serializeResource(episodeOfCare));
            }

            boolean mapIds = !episodeBuilder.isIdMapped();
            fhirResourceFiler.savePatientResource(null, mapIds, episodeBuilder);
        }
        LOG.trace("Finishing saving " + episodeBuildersByEpisodeId.size() + " Episodes to the DB");

        //clear down as everything has been saved
        episodeBuildersByEpisodeId.clear();
    }
}
