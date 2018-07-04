package org.endeavourhealth.transform.barts.cache;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.schema.AEATT;
import org.endeavourhealth.transform.barts.schema.IPEPI;
import org.endeavourhealth.transform.barts.schema.OPATT;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.GenericBuilder;
import org.hl7.fhir.instance.model.EpisodeOfCare;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class EpisodeOfCareResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(EpisodeOfCareResourceCache.class);

    private static final String MAP_ENCOUNTER_TO_EPISODE_ID = "Encounter_to_Episode";
    private static final String MAP_ENCOUNTER_TO_FIN = "Encounter_to_FIN";

    private Map<UUID, EpisodeOfCareBuilder> episodeBuildersByUuid = new HashMap<>();


    /*public void setUpEpisodeOfCareBuilderMappings(ENCNT parser, BartsCsvHelper csvHelper) throws Exception {

        CsvCell encounterIdCell = parser.getEncounterId();
        CsvCell personIdCell = parser.getMillenniumPersonIdentifier();

        //ENCNT has extra columns that allow us to create episodeOfCares
        //For ENCNTs that result of a referral, we have an EPISODE_ID on each record
        //that we can use as the unique ID of the EpisodeOFCare. For non-referral ENCNTs (i.e. ones from A&E and
        //subsequent admissions) there is no EPISODE_ID, so we use the FIN as a proxy for an EPISODE ID
        CsvCell episodeIdCell = parser.getEpisodeIdentifier();
        CsvCell finCell = parser.getMillenniumFinancialNumberIdentifier();

        CsvCell visitIdCell = parser.getVisitId();

        setUpEpisodeOfCareBuilderMappings(encounterIdCell, personIdCell, episodeIdCell, finCell, visitIdCell, csvHelper);
    }*/

    /**
     * when processing the ENCNT file we set up the Episode ID -> UUID and FIN -> UUID mappings for the EpisodeOfCare
     * but don't actually create the Episode itself.
     * Let the transforms for IPEPI, OPATT and AEATT pick up the mappings and create the EpisodeOfCare resource
     */
    public void setUpEpisodeOfCareBuilderMappings(CsvCell encounterIdCell, CsvCell personIdCell, CsvCell episodeIdCell, CsvCell finCell, CsvCell visitIdCell, BartsCsvHelper csvHelper) throws Exception {

        //first, just store the mappings of Encounter ID -> Episode ID and FIN -> Episode ID, so
        //we can always find them later from an Encounter ID (needed because some files only have Encounter ID)
        saveEncounterIdToEpisodeAndFinMappings(encounterIdCell, episodeIdCell, finCell, csvHelper);

        //always prefer to use an Episode ID over a FIN, so check that first
        if (!episodeIdCell.isEmpty()) {

            //see if we've already got an EPISODE ID -> FIN mapping
            String episodeLocalRef = createEpisodeReferenceFromEpisodeId(episodeIdCell);

            //although we have an Episode ID now, we may have previously generated an Episode UUID
            //using the FIN, so check for that too and carry over if found
            if (!finCell.isEmpty()) {

                UUID existingUuidFromEpisodeId = IdHelper.getEdsResourceId(csvHelper.getServiceId(), ResourceType.EpisodeOfCare, episodeLocalRef);
                if (existingUuidFromEpisodeId == null) {

                    String finLocalRef = createEpisodeReferenceFromFin(finCell);
                    UUID existingUuidFromFin = IdHelper.getEdsResourceId(csvHelper.getServiceId(), ResourceType.EpisodeOfCare, finLocalRef);
                    if (existingUuidFromFin != null) {
                        //store against our Episode ID to make the lookup work from both Episode ID and FIN in the future
                        IdHelper.getOrCreateEdsResourceId(csvHelper.getServiceId(), ResourceType.EpisodeOfCare, episodeLocalRef, existingUuidFromFin);
                    }
                }
            }

            //if we've never previously created an Episode UUID for our Episode, generate it now
            ensureUuidExistsForEpisode(personIdCell, episodeIdCell, finCell, visitIdCell, csvHelper);

        } else if (!finCell.isEmpty()) {

            //if we've never previously created an Episode UUID for our Episode, generate it now
            ensureUuidExistsForEpisode(personIdCell, episodeIdCell, finCell, visitIdCell, csvHelper);

        } else {

            //if we have neither Episode ID or FIN, don't create an EpisodeOfCare. If either field
            //is later set on an ENCNT record, the above code will be invoked and an episode created then
        }
    }
    /*public EpisodeOfCareBuilder getEpisodeOfCareBuilder(ENCNT parser, BartsCsvHelper csvHelper) throws Exception {

        CsvCell encounterIdCell = parser.getEncounterId();
        CsvCell personIdCell = parser.getMillenniumPersonIdentifier();
        CsvCell activeIndicatorCell = parser.getActiveIndicator();

        //ENCNT has extra columns that allow us to create episodeOfCares
        //For ENCNTs that result of a referral, we have an EPISODE_ID on each record
        //that we can use as the unique ID of the EpisodeOFCare. For non-referral ENCNTs (i.e. ones from A&E and
        //subsequent admissions) there is no EPISODE_ID, so we use the FIN as a proxy for an EPISODE ID
        CsvCell episodeIdCell = parser.getEpisodeIdentifier();
        CsvCell finCell = parser.getMillenniumFinancialNumberIdentifier();

        //first, just store the mappings of Encounter ID -> Episode ID and FIN -> Episode ID, so
        //we can always find them later from an Encounter ID (needed because some files only have Encounter ID)
        saveEncounterIdToEpisodeAndFinMappings(encounterIdCell, episodeIdCell, finCell, csvHelper);

        //always prefer to use an Episode ID over a FIN, so check that first
        if (!episodeIdCell.isEmpty()) {

            //see if we've already got an EPISODE ID -> FIN mapping
            String episodeLocalRef = createEpisodeReferenceFromEpisodeId(episodeIdCell);

            //although we have an Episode ID now, we may have previously generated an Episode UUID
            //using the FIN, so check for that too and carry over if found
            if (!finCell.isEmpty()) {

                UUID existingUuidFromEpisodeId = IdHelper.getEdsResourceId(csvHelper.getServiceId(), ResourceType.EpisodeOfCare, episodeLocalRef);
                if (existingUuidFromEpisodeId == null) {

                    String finLocalRef = createEpisodeReferenceFromFin(finCell);
                    UUID existingUuidFromFin = IdHelper.getEdsResourceId(csvHelper.getServiceId(), ResourceType.EpisodeOfCare, finLocalRef);
                    if (existingUuidFromFin != null) {
                        //store against our Episode ID to make the lookup work from both Episode ID and FIN in the future
                        IdHelper.getOrCreateEdsResourceId(csvHelper.getServiceId(), ResourceType.EpisodeOfCare, episodeLocalRef, existingUuidFromFin);
                    }
                }
            }

            //if we've never previously created an Episode UUID for our Episode, generate it now
            ensureUuidExistsForEpisode(personIdCell, episodeIdCell, finCell, parser.getVisitId(), csvHelper);

            //retrieve our resource and return
            return retrieveAndCacheBuilder(episodeLocalRef, personIdCell, csvHelper, activeIndicatorCell);

        } else if (!finCell.isEmpty()) {

            //if we've never previously created an Episode UUID for our Episode, generate it now
            ensureUuidExistsForEpisode(personIdCell, episodeIdCell, finCell, parser.getVisitId(), csvHelper);

            //retrieve our resource and return
            String finLocalRef = createEpisodeReferenceFromFin(finCell);
            return retrieveAndCacheBuilder(finLocalRef, personIdCell, csvHelper, activeIndicatorCell);

        } else {

            //if we have neither Episode ID or FIN, don't create an EpisodeOfCare. If either field
            //is later set on an ENCNT record, the above code will be invoked and an episode created then
            return null;
        }
    }*/

    public EpisodeOfCareBuilder getEpisodeOfCareBuilder(AEATT parser, BartsCsvHelper csvHelper) throws Exception {
        CsvCell encounterIdCell = parser.getEncounterId();
        CsvCell personIdCell = parser.getPersonId();
        CsvCell activeIndicatorCell = parser.getActiveIndicator();
        return getEpisodeOfCareBuilder(encounterIdCell, personIdCell, activeIndicatorCell, csvHelper);
    }

    public EpisodeOfCareBuilder getEpisodeOfCareBuilder(OPATT parser, BartsCsvHelper csvHelper) throws Exception {
        CsvCell encounterIdCell = parser.getEncounterId();
        CsvCell personIdCell = parser.getPersonId();
        CsvCell activeIndicatorCell = parser.getActiveIndicator();
        return getEpisodeOfCareBuilder(encounterIdCell, personIdCell, activeIndicatorCell, csvHelper);
    }

    public EpisodeOfCareBuilder getEpisodeOfCareBuilder(IPEPI parser, BartsCsvHelper csvHelper) throws Exception {
        CsvCell encounterIdCell = parser.getEncounterId();
        CsvCell personIdCell = parser.getPersonId();
        CsvCell activeIndicatorCell = parser.getActiveIndicator();
        return getEpisodeOfCareBuilder(encounterIdCell, personIdCell, activeIndicatorCell, csvHelper);
    }

    /**
     * for three of the encounter-related files, there's just an Encounter ID, so we must try to
     * find the EpisodeOfCare by looking up either an Episode ID or FIN using the interal ID map table
     */
    private EpisodeOfCareBuilder getEpisodeOfCareBuilder(CsvCell encounterIdCell, CsvCell personIdCell, CsvCell activeIndicatorCell, BartsCsvHelper csvHelper) throws Exception {

        String encounterId = encounterIdCell.getString();
        String episodeId = csvHelper.getInternalId(MAP_ENCOUNTER_TO_EPISODE_ID, encounterId);
        if (!Strings.isNullOrEmpty(episodeId)) {

            String episodeLocalRef = createEpisodeReferenceFromEpisodeId(episodeId);
            return retrieveAndCacheBuilder(episodeLocalRef, personIdCell, csvHelper, activeIndicatorCell);
        }

        //if we couldn't find an Episode ID for our Encounter ID, see if we can match to a FIN
        String fin = csvHelper.getInternalId(MAP_ENCOUNTER_TO_FIN, encounterId);
        if (!Strings.isNullOrEmpty(fin)) {

            String finLocalRef = createEpisodeReferenceFromFin(fin);
            return retrieveAndCacheBuilder(finLocalRef, personIdCell, csvHelper, activeIndicatorCell);
        }

        return null;
    }

    private static String createEpisodeReferenceFromEpisodeId(CsvCell episodeIdCell) {
        return createEpisodeReferenceFromEpisodeId(episodeIdCell.getString());
    }
    private static String createEpisodeReferenceFromEpisodeId(String episodeId) {
        return "EPISODE_ID:" + episodeId;
    }

    private static String createEpisodeReferenceFromFin(CsvCell finCell) {
        return createEpisodeReferenceFromFin(finCell.getString());
    }
    private static String createEpisodeReferenceFromFin(String fin) {
        return "FIN:" + fin;
    }

    private void saveEncounterIdToEpisodeAndFinMappings(CsvCell encounterIdCell, CsvCell episodeIdcell, CsvCell finCell, BartsCsvHelper csvHelper) throws Exception {

        String encounterId = encounterIdCell.getString();

        //save a mapping of Encounter ID to episode ID so that we can later look up the episode ID just from Encounter ID
        if (!episodeIdcell.isEmpty()) {
            String episodeId = episodeIdcell.getString();

            if (csvHelper.getInternalId(MAP_ENCOUNTER_TO_EPISODE_ID, encounterId) == null) {
                csvHelper.saveInternalId(MAP_ENCOUNTER_TO_EPISODE_ID, encounterId, episodeId);
            }
        }

        //save a mapping of Encounter ID to FIN so that we can later look up the FIN just from Encounter ID
        if (!finCell.isEmpty()) {
            String fin = finCell.getString();

            if (csvHelper.getInternalId(MAP_ENCOUNTER_TO_FIN, encounterId) == null) {
                csvHelper.saveInternalId(MAP_ENCOUNTER_TO_FIN, encounterId, fin);
            }
        }
    }

    private void ensureUuidExistsForEpisode(CsvCell personIdCell, CsvCell episodeIdcell, CsvCell finCell, CsvCell visitIdCell, BartsCsvHelper csvHelper) throws Exception {

        String localEpisodeRef = null;
        if (!episodeIdcell.isEmpty()) {
            localEpisodeRef = createEpisodeReferenceFromEpisodeId(episodeIdcell);
        }
        String localFinRef = null;
        if (!finCell.isEmpty()) {
            localFinRef = createEpisodeReferenceFromFin(finCell);
        }

        //if we have a VISIT ID, then we can potentially match to an EpisodeOfCare from the HL7 receiver, which uses the MRN and VISIT ID
        if (!visitIdCell.isEmpty()) {

            String personId = personIdCell.getString();
            String mrn = csvHelper.getInternalId(InternalIdMap.TYPE_MILLENNIUM_PERSON_ID_TO_MRN, personId);
            if (!Strings.isNullOrEmpty(mrn)) {

                String hl7ReceiverUniqueId = "PIdAssAuth=" + BartsCsvToFhirTransformer.PRIMARY_ORG_HL7_OID + "-PatIdValue=" + mrn + "-EpIdTypeCode=VISITID-EpIdValue=" + visitIdCell.getString(); //this must match the HL7 Receiver
                String hl7ReceiverScope = csvHelper.getHl7ReceiverScope();

                //if we have an episode ID, carry over the ID using that as the local source ID
                if (!Strings.isNullOrEmpty(localEpisodeRef)) {
                    csvHelper.createResourceIdOrCopyFromHl7Receiver(ResourceType.EpisodeOfCare, localEpisodeRef, hl7ReceiverUniqueId, hl7ReceiverScope, false);
                }

                //and if we have a FIN, then call the same function but using the local FIN
                //which will result in our ID->UUID table having the same UUID mapped to by the FIN as well
                if (!Strings.isNullOrEmpty(localFinRef)) {
                    csvHelper.createResourceIdOrCopyFromHl7Receiver(ResourceType.EpisodeOfCare, localFinRef, hl7ReceiverUniqueId, hl7ReceiverScope, false);
                }

                return;
            }
        }

        //if we couldn't match to the HL7 Receiver using the VISIT ID and MRN, then just generate a new episode UUID
        //and store against both Episode ID and FIN if we have them
        if (!Strings.isNullOrEmpty(localEpisodeRef)) {
            //if we have an episode ID, generate the UUID using that
            UUID uuid = IdHelper.getOrCreateEdsResourceId(csvHelper.getServiceId(), ResourceType.EpisodeOfCare, localEpisodeRef);

            //and if we have a FIN, then carry over the same UUID to be mapped against the FIN too
            if (!Strings.isNullOrEmpty(localFinRef)) {
                IdHelper.getOrCreateEdsResourceId(csvHelper.getServiceId(), ResourceType.EpisodeOfCare, localEpisodeRef, uuid);
            }

        } else {
            //if we just have a FIN, generate a UUID against it
            IdHelper.getOrCreateEdsResourceId(csvHelper.getServiceId(), ResourceType.EpisodeOfCare, localFinRef);
        }
    }

    private EpisodeOfCareBuilder retrieveAndCacheBuilder(String localRef, CsvCell personIdCell, BartsCsvHelper csvHelper, CsvCell activeIndicatorCell) throws Exception {

        //the local ref may be an Episode ID or FIN, but both mappings should be created in the source ID -> UUID map table
        UUID episodeUuid = IdHelper.getEdsResourceId(csvHelper.getServiceId(), ResourceType.EpisodeOfCare, localRef);

        EpisodeOfCareBuilder builder = episodeBuildersByUuid.get(episodeUuid);
        if (builder == null) {

            EpisodeOfCare episodeOfCare = (EpisodeOfCare)csvHelper.retrieveResourceForUuid(ResourceType.EpisodeOfCare, episodeUuid);
            if (episodeOfCare != null) {
                builder = new EpisodeOfCareBuilder(episodeOfCare);

                //always update the patient and managing org reference, in case we've merged records, remembering to map from local ID to UUID since this has already been ID mapped
                Reference patientReference = csvHelper.createPatientReference(personIdCell);
                patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, csvHelper);
                builder.setPatient(patientReference, personIdCell);

                String orgId = csvHelper.findOrgRefIdForBarts();
                Reference orgReference = ReferenceHelper.createReference(ResourceType.Organization, orgId);
                orgReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(orgReference, csvHelper);
                builder.setManagingOrganisation(orgReference);

            } else {
                //if we're deleting something, don't start creating a new episodeOfCare
                if (!activeIndicatorCell.getIntAsBoolean()) {
                    return null;
                }

                builder = new EpisodeOfCareBuilder();
                builder.setId(localRef);

                String orgId = csvHelper.findOrgRefIdForBarts();
                Reference orgReference = ReferenceHelper.createReference(ResourceType.Organization, orgId);
                builder.setManagingOrganisation(orgReference);

                Reference patientReference = csvHelper.createPatientReference(personIdCell);
                builder.setPatient(patientReference, personIdCell);
            }

            episodeBuildersByUuid.put(episodeUuid, builder);
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


    public void fileResources(FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        //before saving the new ones work out any patients that we've changed
        Set<String> hsPatientUuidsChanged = new HashSet<>();

        LOG.trace("Saving " + episodeBuildersByUuid.size() + " Episodes to the DB");
        for (UUID episodeUuid: episodeBuildersByUuid.keySet()) {
            EpisodeOfCareBuilder episodeBuilder = episodeBuildersByUuid.get(episodeUuid);

            //find the patient UUID for the encounter, so we can tidy up HL7 encounters after doing all the saving
            Reference patientReference = episodeBuilder.getPatient();
            if (!episodeBuilder.isIdMapped()) {
                patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
            }
            String patientUuid = ReferenceHelper.getReferenceId(patientReference);
            hsPatientUuidsChanged.add(patientUuid);

            //and save our new resource
            boolean mapIds = !episodeBuilder.isIdMapped();
            fhirResourceFiler.savePatientResource(null, mapIds, episodeBuilder);
        }
        LOG.trace("Finishing saving Episodes to the DB");

        //clear down as everything has been saved
        episodeBuildersByUuid.clear();

        //now delete any older HL7 Encounters for patients we've updated
        //but waiting until everything has been saved to the DB first
        fhirResourceFiler.waitUntilEverythingIsSaved();

        for (String patientUuid: hsPatientUuidsChanged) {
            deleteHl7ReceiverEpisodes(UUID.fromString(patientUuid), fhirResourceFiler, csvHelper);
        }
    }

    /**
     * we match to some HL7 Receiver Episodes, taking them over (i.e. changing the system ID to our own)
     * so we call this to tidy up (delete) any Episodes left not taken over, as the HL7 Receiver creates too many
     * episodes because it doesn't have the data to avoid doing so
     */
    private void deleteHl7ReceiverEpisodes(UUID patientUuid, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        UUID serviceUuid = fhirResourceFiler.getServiceId();
        UUID systemUuid = fhirResourceFiler.getSystemId();

        //we want to delete any HL7 Encounter more than 24 hours older than the DW file extract date
        Date extractDateTime = csvHelper.getExtractDateTime();
        Date cutoff = new Date(extractDateTime.getTime() - (24 * 60 * 60 * 1000));

        ResourceDalI resourceDal = DalProvider.factoryResourceDal();
        List<ResourceWrapper> resourceWrappers = resourceDal.getResourcesByPatient(serviceUuid, patientUuid, ResourceType.EpisodeOfCare.toString());
        for (ResourceWrapper wrapper: resourceWrappers) {

            //if this episode is for our own system ID (i.e. DW feed), then leave it
            UUID wrapperSystemId = wrapper.getSystemId();
            if (wrapperSystemId.equals(systemUuid)) {
                continue;
            }

            String json = wrapper.getResourceData();
            EpisodeOfCare existingEpisode = (EpisodeOfCare)FhirSerializationHelper.deserializeResource(json);
            fhirResourceFiler.deletePatientResource(null, false, new GenericBuilder(existingEpisode));
        }
    }
}
