package org.endeavourhealth.transform.barts.cache;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.schema.AEATT;
import org.endeavourhealth.transform.barts.schema.ENCNT;
import org.endeavourhealth.transform.barts.schema.IPEPI;
import org.endeavourhealth.transform.barts.schema.OPATT;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.ParserI;
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

    private Map<String, EpisodeOfCareBuilder> episodeBuildersByEpisodeId = new HashMap<>();

    /**
     * retrieves or creates an EpisodeOfCareBuilder, using one of three identifiers
     */
    public EpisodeOfCareBuilder getEpisodeOfCareBuilder(ParserI parser, BartsCsvHelper csvHelper) throws Exception {

        CsvCell encounterIdCell = null;
        CsvCell personIdCell = null;
        CsvCell activeIndicatorCell = null;

        if (parser instanceof ENCNT) {
            ENCNT p = (ENCNT)parser;
            encounterIdCell = p.getEncounterId();
            personIdCell = p.getMillenniumPersonIdentifier();
            activeIndicatorCell = p.getActiveIndicator();

            //ENCNT has extra columns that allow us to create episodeOfCares
            //we want to create an EpisodeOfCare as early as possible, but don't always have an episode ID that
            //early. If we don't have an episode ID, then we don't create an episodeOfCare
            CsvCell episodeIdcell = p.getEpisodeIdentifier();
            if (!BartsCsvHelper.isEmptyOrIsZero(episodeIdcell)) {

                //make sure a UUID exists for the EpisodeOfCare because the reference on Encounters will be set
                //before the Episode gets ID mapped. So do this to force the Episode ID -> UUID mapping is saved now
                ensureUuidExistsForEpisode(personIdCell, episodeIdcell, p.getVisitId(), csvHelper);

                //save a mapping of Encounter ID to episode ID so that we can later look up the episode ID just from Encounter ID
                String episodeId = episodeIdcell.getString();
                String encounterId = encounterIdCell.getString();
                if (csvHelper.getInternalId(InternalIdMap.TYPE_ENCOUNTER_ID_TO_EPISODE_UUID, encounterId) == null) {
                    csvHelper.saveInternalId(InternalIdMap.TYPE_ENCOUNTER_ID_TO_EPISODE_UUID, encounterId, episodeId);
                }

                //retrieve our resource and return
                return retrieveAndCacheBuilder(episodeId, personIdCell, csvHelper, activeIndicatorCell);
            }

        } else if (parser instanceof AEATT) {
            AEATT p = (AEATT)parser;
            encounterIdCell = p.getEncounterId();
            personIdCell = p.getMillenniumPersonIdentifier();
            activeIndicatorCell = p.getActiveIndicator();

        } else if (parser instanceof IPEPI) {
            IPEPI p = (IPEPI)parser;
            encounterIdCell = p.getEncounterId();
            personIdCell = p.getPatientId();
            activeIndicatorCell = p.getActiveIndicator();

        } else if (parser instanceof OPATT) {
            OPATT p = (OPATT)parser;
            encounterIdCell = p.getEncounterId();
            personIdCell = p.getPersonId();
            activeIndicatorCell = p.getActiveIndicator();

        } else {
            throw new TransformException("Unexpected parser type " + parser.getClass());
        }

        //if we don't have an Episode ID, then just try looking up by Encounter ID using the mapping created above
        String encounterId = encounterIdCell.getString();
        String episodeId = csvHelper.getInternalId(InternalIdMap.TYPE_ENCOUNTER_ID_TO_EPISODE_UUID, encounterId);
        if (!Strings.isNullOrEmpty(episodeId)) {

            return retrieveAndCacheBuilder(episodeId, personIdCell, csvHelper, activeIndicatorCell);
        }

        return null;
    }

    private void ensureUuidExistsForEpisode(CsvCell personIdCell, CsvCell episodeIdcell, CsvCell visitIdCell, BartsCsvHelper csvHelper) throws Exception {

        //if we have a VISIT ID, then we can potentially match to an EpisodeOfCare from the HL7 receiver, which uses the MRN and VISIT ID
        if (!visitIdCell.isEmpty()) {

            String personId = personIdCell.getString();
            String mrn = csvHelper.getInternalId(InternalIdMap.TYPE_MILLENNIUM_PERSON_ID_TO_MRN, personId);
            if (!Strings.isNullOrEmpty(mrn)) {

                String localUniqueId = episodeIdcell.getString();
                String hl7ReceiverUniqueId = "PIdAssAuth=" + BartsCsvToFhirTransformer.PRIMARY_ORG_HL7_OID + "-PatIdValue=" + mrn + "-EpIdTypeCode=VISITID-EpIdValue=" + visitIdCell.getString(); //this must match the HL7 Receiver
                String hl7ReceiverScope = csvHelper.getHl7ReceiverScope();
                csvHelper.createResourceIdOrCopyFromHl7Receiver(ResourceType.EpisodeOfCare, localUniqueId, hl7ReceiverUniqueId, hl7ReceiverScope);

                return;
            }
        }

        //if we couldn't match to the HL7 Receiver using the VISIT ID and MRN, then just generate a new one
        String episodeId = episodeIdcell.getString();
        IdHelper.getOrCreateEdsResourceId(csvHelper.getServiceId(), ResourceType.EpisodeOfCare, episodeId, UUID.randomUUID());
    }

    private EpisodeOfCareBuilder retrieveAndCacheBuilder(String episodeId, CsvCell personIdCell, BartsCsvHelper csvHelper, CsvCell activeIndicatorCell) throws Exception {

        EpisodeOfCareBuilder builder = episodeBuildersByEpisodeId.get(episodeId);
        if (builder == null) {

            EpisodeOfCare episodeOfCare = (EpisodeOfCare)csvHelper.retrieveResourceForLocalId(ResourceType.EpisodeOfCare, episodeId);
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


    public void fileResources(FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        //before saving the new ones work out any patients that we've changed
        Set<String> hsPatientUuidsChanged = new HashSet<>();

        LOG.trace("Saving " + episodeBuildersByEpisodeId.size() + " Episodes to the DB");
        for (String episodeId: episodeBuildersByEpisodeId.keySet()) {
            EpisodeOfCareBuilder episodeBuilder = episodeBuildersByEpisodeId.get(episodeId);

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
        LOG.trace("Finishing saving " + episodeBuildersByEpisodeId.size() + " Episodes to the DB");

        //clear down as everything has been saved
        episodeBuildersByEpisodeId.clear();

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
