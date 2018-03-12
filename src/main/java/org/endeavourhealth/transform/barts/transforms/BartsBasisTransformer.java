package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.core.database.dal.publisherTransform.InternalIdDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.cache.EncounterResourceCache;
import org.endeavourhealth.transform.common.BasisTransformer;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.hl7.fhir.instance.model.Enumerations;
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

    public static EpisodeOfCareBuilder readOrCreateEpisodeOfCareBuilder(CsvCell episodeIdentiferCell, CsvCell finIdCell, CsvCell encounterIdCell, BartsCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler, InternalIdDalI internalIdDAL) throws Exception {
        String alternateEpisodeUUID = null;
        EpisodeOfCareBuilder episodeOfCareBuilder = null;

        if (episodeIdentiferCell != null && !episodeIdentiferCell.isEmpty()) {
            episodeOfCareBuilder = EncounterResourceCache.getEpisodeBuilder(csvHelper, episodeIdentiferCell.getString());

            if (episodeOfCareBuilder == null && finIdCell != null && !finIdCell.isEmpty()) {
                // EoC not found using Episode-id - try FIN-no (if it was created before it got the episode id)
                alternateEpisodeUUID = internalIdDAL.getDestinationId(fhirResourceFiler.getServiceId(), InternalIdMap.TYPE_FIN_NO_TO_EPISODE_UUID, finIdCell.getString());
                if (alternateEpisodeUUID != null) {
                    episodeOfCareBuilder = EncounterResourceCache.getEpisodeBuilder(csvHelper, alternateEpisodeUUID);
                    if (episodeOfCareBuilder != null) {
                        // Save the resource UUID so it can be found using episode-id next time
                        createEpisodeOfCareResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, episodeIdentiferCell.getString(), UUID.fromString(alternateEpisodeUUID));
                    }
                }
            }
            if (episodeOfCareBuilder == null) {
                // EoC not found using Episode-id or FIN-no - try Encounter-id (if it was created before it got the episode id and FIN no)
                alternateEpisodeUUID = internalIdDAL.getDestinationId(fhirResourceFiler.getServiceId(), InternalIdMap.TYPE_ENCOUNTER_ID_TO_EPISODE_UUID, encounterIdCell.getString());
                if (alternateEpisodeUUID != null) {
                    episodeOfCareBuilder = EncounterResourceCache.getEpisodeBuilder(csvHelper, alternateEpisodeUUID);
                    if (episodeOfCareBuilder != null) {
                        // Save the resource UUID so it can be found using episode-id next time
                        createEpisodeOfCareResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, episodeIdentiferCell.getString(), UUID.fromString(alternateEpisodeUUID));
                    }
                }
            }
            if (episodeOfCareBuilder == null) {
                // EoC not  found - create new using episode-id
                episodeOfCareBuilder = EncounterResourceCache.createEpisodeBuilder(episodeIdentiferCell);
            }
        } else if (finIdCell != null && !finIdCell.isEmpty()) {
            // Episode-id not present - use FIN NO
            alternateEpisodeUUID = internalIdDAL.getDestinationId(fhirResourceFiler.getServiceId(), InternalIdMap.TYPE_FIN_NO_TO_EPISODE_UUID, finIdCell.getString());
            if (alternateEpisodeUUID == null) {
                episodeOfCareBuilder = new EpisodeOfCareBuilder();
                episodeOfCareBuilder.setId(UUID.randomUUID().toString(), finIdCell);
                EncounterResourceCache.saveNewEpisodeBuilderToCache(episodeOfCareBuilder);

                internalIdDAL.insertRecord(fhirResourceFiler.getServiceId(), InternalIdMap.TYPE_FIN_NO_TO_EPISODE_UUID, finIdCell.getString(), episodeOfCareBuilder.getResourceId());
            } else {
                episodeOfCareBuilder = EncounterResourceCache.getEpisodeBuilder(csvHelper, alternateEpisodeUUID);
                if (episodeOfCareBuilder == null) {
                    episodeOfCareBuilder = new EpisodeOfCareBuilder();
                    episodeOfCareBuilder.setId(alternateEpisodeUUID, finIdCell);
                    EncounterResourceCache.saveNewEpisodeBuilderToCache(episodeOfCareBuilder);
                }
            }
        } else {
            // Neither Episode-id nor FIN No present - use encounter-id
            alternateEpisodeUUID = internalIdDAL.getDestinationId(fhirResourceFiler.getServiceId(), InternalIdMap.TYPE_ENCOUNTER_ID_TO_EPISODE_UUID, encounterIdCell.getString());
            if (alternateEpisodeUUID == null) {
                episodeOfCareBuilder = new EpisodeOfCareBuilder();
                episodeOfCareBuilder.setId(UUID.randomUUID().toString(), episodeIdentiferCell);
                EncounterResourceCache.saveNewEpisodeBuilderToCache(episodeOfCareBuilder);

                internalIdDAL.insertRecord(fhirResourceFiler.getServiceId(), InternalIdMap.TYPE_ENCOUNTER_ID_TO_EPISODE_UUID, encounterIdCell.getString(), episodeOfCareBuilder.getResourceId());
            } else {
                episodeOfCareBuilder = EncounterResourceCache.getEpisodeBuilder(csvHelper, alternateEpisodeUUID);
                if (episodeOfCareBuilder == null) {
                    episodeOfCareBuilder = new EpisodeOfCareBuilder();
                    episodeOfCareBuilder.setId(alternateEpisodeUUID, encounterIdCell);
                    EncounterResourceCache.saveNewEpisodeBuilderToCache(episodeOfCareBuilder);
                }
            }
        }

        return episodeOfCareBuilder;
    }

}
