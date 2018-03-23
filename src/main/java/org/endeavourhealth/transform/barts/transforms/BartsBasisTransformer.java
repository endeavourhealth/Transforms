package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherTransform.InternalIdDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.cache.EncounterResourceCache;
import org.endeavourhealth.transform.common.BasisTransformer;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.hl7.fhir.instance.model.Enumerations;
import org.hl7.fhir.instance.model.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.UUID;

public class BartsBasisTransformer extends BasisTransformer{
    private static final Logger LOG = LoggerFactory.getLogger(BartsBasisTransformer.class);

    /*
    private static Connection hl7receiverConnection = null;
    private static PreparedStatement resourceIdSelectStatement;
    private static PreparedStatement resourceIdInsertStatement;
    private static PreparedStatement mappingSelectStatement;
    private static PreparedStatement getAllCodeSystemsSelectStatement;
    private static PreparedStatement getCodeSystemsSelectStatement;
    private static HashMap<Integer, String> codeSystemCache = new HashMap<Integer, String>();
    private static int lastLookupCodeSystemId = 0;
    private static String lastLookupCodeSystemIdentifier = "";
    */

    public static Enumerations.AdministrativeGender convertSusGenderToFHIR(int gender) {
        if (gender == 1) {
            return Enumerations.AdministrativeGender.MALE;
        } else {
            if (gender == 2) {
                return Enumerations.AdministrativeGender.FEMALE;
            } else {
                if (gender == 9) {
                    return Enumerations.AdministrativeGender.NULL;
                } else {
                    return Enumerations.AdministrativeGender.UNKNOWN;
                }
            }
        }
    }

    public static String getSusEthnicCategoryDisplay(String ethnicGroup) {
        if (ethnicGroup.compareToIgnoreCase("A") == 0) {
            return "British";
        } else if (ethnicGroup.compareToIgnoreCase("B") == 0) {
            return "Irish";
        } else if (ethnicGroup.compareToIgnoreCase("C") == 0) {
            return "Any other White background";
        } else if (ethnicGroup.compareToIgnoreCase("D") == 0) {
            return "White and Black Caribbean";
        } else if (ethnicGroup.compareToIgnoreCase("E") == 0) {
            return "White and Black African";
        } else if (ethnicGroup.compareToIgnoreCase("F") == 0) {
            return "White and Asian";
        } else if (ethnicGroup.compareToIgnoreCase("G") == 0) {
            return "Any other mixed background";
        } else if (ethnicGroup.compareToIgnoreCase("H") == 0) {
            return "Indian";
        } else if (ethnicGroup.compareToIgnoreCase("J") == 0) {
            return "Pakistani";
        } else if (ethnicGroup.compareToIgnoreCase("K") == 0) {
            return "Bangladeshi";
        } else if (ethnicGroup.compareToIgnoreCase("L") == 0) {
            return "Any other Asian background";
        } else if (ethnicGroup.compareToIgnoreCase("M") == 0) {
            return "Caribbean";
        } else if (ethnicGroup.compareToIgnoreCase("N") == 0) {
            return "African";
        } else if (ethnicGroup.compareToIgnoreCase("P") == 0) {
            return "Any other Black background";
        } else if (ethnicGroup.compareToIgnoreCase("R") == 0) {
            return "Chinese";
        } else if (ethnicGroup.compareToIgnoreCase("S") == 0) {
            return "Any other ethnic group";
        } else if (ethnicGroup.compareToIgnoreCase("Z") == 0) {
            return "Not stated";
        } else {
            return "";
        }
    }

    /*
    * Set unknown values to null
    * For non-AE encounters set 'aeArrivalDateTime' to null
     */
    public static EpisodeOfCareBuilder readOrCreateEpisodeOfCareBuilder(CsvCell episodeIdentiferCell, CsvCell finIdCell, CsvCell encounterIdCell, CsvCell personIdCell, CsvCell aeArrivalDateTime, BartsCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler, InternalIdDalI internalIdDAL) throws Exception {
        String FINalternateEpisodeUUID = null;
        String encounterAlternateEpisodeUUID = null;
        EpisodeOfCareBuilder episodeOfCareBuilder = null;

        if (episodeIdentiferCell != null && !episodeIdentiferCell.isEmpty()) {
            episodeOfCareBuilder = EncounterResourceCache.getEpisodeBuilder(csvHelper, episodeIdentiferCell.getString());
            if (episodeOfCareBuilder == null) {
                LOG.debug("episodeOfCareBuilder not found for id:" + episodeIdentiferCell.getString());
            }

            if (episodeOfCareBuilder == null && finIdCell != null && !finIdCell.isEmpty()) {
                // EoC not found using Episode-id - try FIN-no (if it was created before it got the episode id)
                FINalternateEpisodeUUID = internalIdDAL.getDestinationId(fhirResourceFiler.getServiceId(), InternalIdMap.TYPE_FIN_NO_TO_EPISODE_UUID, finIdCell.getString());
                if (FINalternateEpisodeUUID != null) {
                    episodeOfCareBuilder = EncounterResourceCache.getEpisodeBuilder(csvHelper, FINalternateEpisodeUUID);
                    if (episodeOfCareBuilder != null) {
                        // Save the resource UUID so it can be found using episode-id next time
                        createEpisodeOfCareResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, episodeIdentiferCell.getString(), UUID.fromString(FINalternateEpisodeUUID));
                    }
                }
            }
            if (episodeOfCareBuilder == null) {
                // EoC not found using Episode-id or FIN-no - try Encounter-id (if it was created before it got the episode id and FIN no)
                encounterAlternateEpisodeUUID = internalIdDAL.getDestinationId(fhirResourceFiler.getServiceId(), InternalIdMap.TYPE_ENCOUNTER_ID_TO_EPISODE_UUID, encounterIdCell.getString());
                if (encounterAlternateEpisodeUUID != null) {
                    episodeOfCareBuilder = EncounterResourceCache.getEpisodeBuilder(csvHelper, encounterAlternateEpisodeUUID);
                    if (episodeOfCareBuilder != null) {
                        // Save the resource UUID so it can be found using episode-id next time
                        createEpisodeOfCareResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, episodeIdentiferCell.getString(), UUID.fromString(encounterAlternateEpisodeUUID));
                    }
                }
            }
            if (episodeOfCareBuilder == null) {
                // EoC not  found - create new using episode-id
                ResourceId resourceId = getEpisodeOfCareResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, episodeIdentiferCell.getString());
                if (resourceId == null) {
                    resourceId = createEpisodeOfCareResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, episodeIdentiferCell.getString());
                }
                episodeOfCareBuilder = createNewEpisodeOfCareBuilder(episodeIdentiferCell);
                episodeOfCareBuilder.setId(resourceId.getResourceId().toString(), episodeIdentiferCell);
                EncounterResourceCache.saveNewEpisodeBuilderToCache(episodeOfCareBuilder);

                if (FINalternateEpisodeUUID == null && finIdCell != null && !finIdCell.isEmpty()) {
                    internalIdDAL.insertRecord(fhirResourceFiler.getServiceId(), InternalIdMap.TYPE_FIN_NO_TO_EPISODE_UUID, finIdCell.getString(), episodeOfCareBuilder.getResourceId());
                }
                if (encounterAlternateEpisodeUUID == null) {
                    internalIdDAL.insertRecord(fhirResourceFiler.getServiceId(), InternalIdMap.TYPE_ENCOUNTER_ID_TO_EPISODE_UUID, encounterIdCell.getString(), episodeOfCareBuilder.getResourceId());
                }
            }
        } else if (finIdCell != null && !finIdCell.isEmpty()) {
            // Episode-id not present - use FIN NO
            FINalternateEpisodeUUID = internalIdDAL.getDestinationId(fhirResourceFiler.getServiceId(), InternalIdMap.TYPE_FIN_NO_TO_EPISODE_UUID, finIdCell.getString());
            if (FINalternateEpisodeUUID == null) {
                episodeOfCareBuilder = createNewEpisodeOfCareBuilder(episodeIdentiferCell);
                episodeOfCareBuilder.setId(UUID.randomUUID().toString(), finIdCell);
                EncounterResourceCache.saveNewEpisodeBuilderToCache(episodeOfCareBuilder);

                internalIdDAL.insertRecord(fhirResourceFiler.getServiceId(), InternalIdMap.TYPE_FIN_NO_TO_EPISODE_UUID, finIdCell.getString(), episodeOfCareBuilder.getResourceId());
            } else {
                episodeOfCareBuilder = EncounterResourceCache.getEpisodeBuilder(csvHelper, FINalternateEpisodeUUID);
                if (episodeOfCareBuilder == null) {
                    episodeOfCareBuilder = createNewEpisodeOfCareBuilder(episodeIdentiferCell);
                    episodeOfCareBuilder.setId(FINalternateEpisodeUUID, finIdCell);
                    EncounterResourceCache.saveNewEpisodeBuilderToCache(episodeOfCareBuilder);
                }
            }
        }
        /* Encounter-id always seem to be present - probably more reliable to just use that
        else if (aeArrivalDateTime != null) {
            String aekey = personIdCell.getString() + InternalIdMap.KEY_SPLIT_CHAR + aeArrivalDateTime.getString();
            alternateEpisodeUUID = internalIdDAL.getDestinationId(fhirResourceFiler.getServiceId(), InternalIdMap.TYPE_AE_ARRIVAL_DT_TM_TO_EPISODE_UUID, aekey);
            if (alternateEpisodeUUID == null) {
                episodeOfCareBuilder = createNewEpisodeOfCareBuilder(episodeIdentiferCell, finIdCell);
                episodeOfCareBuilder.setId(UUID.randomUUID().toString(), personIdCell, aeArrivalDateTime);
                EncounterResourceCache.saveNewEpisodeBuilderToCache(episodeOfCareBuilder);

                internalIdDAL.insertRecord(fhirResourceFiler.getServiceId(), InternalIdMap.TYPE_AE_ARRIVAL_DT_TM_TO_EPISODE_UUID, aekey, episodeOfCareBuilder.getResourceId());
            } else {
                episodeOfCareBuilder = EncounterResourceCache.getEpisodeBuilder(csvHelper, alternateEpisodeUUID);
                if (episodeOfCareBuilder == null) {
                    episodeOfCareBuilder = createNewEpisodeOfCareBuilder(episodeIdentiferCell, finIdCell);
                    episodeOfCareBuilder.setId(alternateEpisodeUUID, personIdCell, aeArrivalDateTime);
                    EncounterResourceCache.saveNewEpisodeBuilderToCache(episodeOfCareBuilder);
                }
            }
        }*/
        else {
            // Neither Episode-id nor FIN No present - use encounter-id
            encounterAlternateEpisodeUUID = internalIdDAL.getDestinationId(fhirResourceFiler.getServiceId(), InternalIdMap.TYPE_ENCOUNTER_ID_TO_EPISODE_UUID, encounterIdCell.getString());
            if (encounterAlternateEpisodeUUID == null) {
                episodeOfCareBuilder = createNewEpisodeOfCareBuilder(episodeIdentiferCell);
                episodeOfCareBuilder.setId(UUID.randomUUID().toString(), episodeIdentiferCell);
                EncounterResourceCache.saveNewEpisodeBuilderToCache(episodeOfCareBuilder);

                internalIdDAL.insertRecord(fhirResourceFiler.getServiceId(), InternalIdMap.TYPE_ENCOUNTER_ID_TO_EPISODE_UUID, encounterIdCell.getString(), episodeOfCareBuilder.getResourceId());
            } else {
                episodeOfCareBuilder = EncounterResourceCache.getEpisodeBuilder(csvHelper, encounterAlternateEpisodeUUID);
                if (episodeOfCareBuilder == null) {
                    episodeOfCareBuilder = createNewEpisodeOfCareBuilder(episodeIdentiferCell);
                    episodeOfCareBuilder.setId(encounterAlternateEpisodeUUID, encounterIdCell);
                    EncounterResourceCache.saveNewEpisodeBuilderToCache(episodeOfCareBuilder);
                }
            }
        }

        return episodeOfCareBuilder;
    }

    private static EpisodeOfCareBuilder createNewEpisodeOfCareBuilder(CsvCell episodeIdentiferCell) {
        EpisodeOfCareBuilder episodeOfCareBuilder = new EpisodeOfCareBuilder();
        if (episodeIdentiferCell != null && !episodeIdentiferCell.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(episodeOfCareBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.OFFICIAL);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_CERNER_EPISODE_ID);
            identifierBuilder.setValue(episodeIdentiferCell.getString(), episodeIdentiferCell);
        }

        return episodeOfCareBuilder;
    }

}
