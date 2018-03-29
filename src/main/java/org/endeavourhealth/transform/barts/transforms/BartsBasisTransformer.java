package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
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
import org.hl7.fhir.instance.model.EpisodeOfCare;
import org.hl7.fhir.instance.model.Identifier;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.UUID;

public class BartsBasisTransformer extends BasisTransformer{
    private static final Logger LOG = LoggerFactory.getLogger(BartsBasisTransformer.class);

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
    public static EpisodeOfCareBuilder readOrCreateEpisodeOfCareBuilder(CsvCell episodeIdentiferCell, CsvCell finIdCell, CsvCell encounterIdCell, CsvCell personIdCell, UUID personUUID, BartsCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {
        return readOrCreateEpisodeOfCareBuilder(episodeIdentiferCell, finIdCell, encounterIdCell, personIdCell, personUUID, null, csvHelper, fhirResourceFiler, null);
    }

    public static EpisodeOfCareBuilder readOrCreateEpisodeOfCareBuilder(CsvCell episodeIdentiferCell, CsvCell finIdCell, CsvCell encounterIdCell, CsvCell personIdCell, UUID personUUID, CsvCell aeArrivalDateTime, BartsCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler, InternalIdDalI internalIdDAL) throws Exception {
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
            episodeOfCareBuilder = EncounterResourceCache.getEpisodeBuilder(csvHelper, episodeIdentiferCellString);
            if (episodeOfCareBuilder == null) {
                LOG.debug("episodeOfCareBuilder not found for id:" + episodeIdentiferCellString);
            }

            if (episodeOfCareBuilder == null && finIdCellString != null) {
                // EoC not found using Episode-id - try FIN-no (if it was created before it got the episode id)
                FINalternateEpisodeUUID = csvHelper.getInternalId(InternalIdMap.TYPE_FIN_NO_TO_EPISODE_UUID, finIdCell.getString());
                if (FINalternateEpisodeUUID != null) {
                    episodeOfCareBuilder = EncounterResourceCache.getEpisodeBuilder(csvHelper, UUID.fromString(FINalternateEpisodeUUID));
                    if (episodeOfCareBuilder != null) {
                        // Save the resource UUID so it can be found using episode-id next time
                        createEpisodeOfCareResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, episodeIdentiferCellString, UUID.fromString(FINalternateEpisodeUUID));
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
                        createEpisodeOfCareResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, episodeIdentiferCellString, UUID.fromString(encounterAlternateEpisodeUUID));
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
                EncounterResourceCache.saveNewEpisodeBuilderToCache(episodeOfCareBuilder);
                newEoCCreated = true;
            }
        } else if (finIdCellString != null) {
            LOG.debug("Search using FINNo");
            // Episode-id not present - use FIN NO
            FINalternateEpisodeUUID = csvHelper.getInternalId(InternalIdMap.TYPE_FIN_NO_TO_EPISODE_UUID, finIdCellString);
            if (FINalternateEpisodeUUID == null) {
                episodeOfCareBuilder = createNewEpisodeOfCareBuilder(episodeIdentiferCell, personIdCell, personUUID, finIdCell, null);
                episodeOfCareBuilder.setId(UUID.randomUUID().toString(), finIdCell);
                EncounterResourceCache.saveNewEpisodeBuilderToCache(episodeOfCareBuilder);
                newEoCCreated = true;
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
    }

    private static EpisodeOfCareBuilder createNewEpisodeOfCareBuilder(CsvCell episodeIdentiferCell, CsvCell personIdCell, UUID personUUID, CsvCell finIdCell, CsvCell encounterIdCell) {
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
    }

}
