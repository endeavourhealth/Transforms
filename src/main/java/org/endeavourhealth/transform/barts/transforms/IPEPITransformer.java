package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.cache.EncounterResourceCache;
import org.endeavourhealth.transform.barts.schema.IPEPI;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.EpisodeOfCare;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class IPEPITransformer {
    private static final Logger LOG = LoggerFactory.getLogger(IPEPITransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {

                //no try/catch as records in this file aren't independent and can't be re-processed on their own
                if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser)parser)) {
                    continue;
                }
                createEpisodeEvent((IPEPI) parser, fhirResourceFiler, csvHelper);
            }
        }
    }

    public static void createEpisodeEvent(IPEPI parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        CsvCell activeCell = parser.getActiveIndicator();
        if (!activeCell.getIntAsBoolean()) {
            //if the record is non-active (i.e. deleted) then we don't get any other columns. But we can also expect that our linked
            //ENCNT record will be deleted too, so we don't need to do anything extra here
            return;
        }

        CsvCell encounterIdCell = parser.getEncounterId();
        CsvCell personIdCell = parser.getPersonId();

        // get the associated encounter
        EncounterBuilder encounterBuilder = csvHelper.getEncounterCache().borrowEncounterBuilder(encounterIdCell, personIdCell, activeCell);

        //in Cerner a when a patient visits A&E and is admitted, that's the SAME encounter record that gets updated.
        //To allow us to keep separate Emergency and Inpatient FHIR Encounters, we need to detect and Emergency Encounter
        //that is about to change to being an Inpatient one. Check this using the class, which will either have been
        //set in a previous Exchange or by the AEATT transformer that just
        //if the Encounter already has a class of "Emergency", then it means it was an A&E attendance that resulted in an inpatient admission
        if (((Encounter)encounterBuilder.getResource()).getClass_() == Encounter.EncounterClass.EMERGENCY) {
            createEmegencyEncounterResourceCopy(encounterBuilder, encounterIdCell, csvHelper);
        }

        encounterBuilder.setClass(Encounter.EncounterClass.INPATIENT);


        CsvCell beginDateCell = parser.getEpisodeStartDateTime();
        CsvCell endDateCell = parser.getEpisodeEndDateTime();

        Date beginDate = null;
        if (!BartsCsvHelper.isEmptyOrIsStartOfTime(beginDateCell)) {
            beginDate = BartsCsvHelper.parseDate(beginDateCell);
        }
        Date endDate = null;
        if (!BartsCsvHelper.isEmptyOrIsEndOfTime(endDateCell)) {
            endDate = BartsCsvHelper.parseDate(endDateCell);
        }

        if (beginDate != null) {
            // Start date
            encounterBuilder.setPeriodStart(beginDate);

            // End date
            if (endDate != null) {
                encounterBuilder.setPeriodEnd(endDate);

                encounterBuilder.setStatus(Encounter.EncounterState.FINISHED);

            } else if (beginDate.before(new Date())) {
                encounterBuilder.setStatus(Encounter.EncounterState.INPROGRESS, beginDateCell);
            } else {
                encounterBuilder.setStatus(Encounter.EncounterState.PLANNED, beginDateCell);
            }
        } else {
            encounterBuilder.setStatus(Encounter.EncounterState.PLANNED);
        }

        //EpisodOfCare
        EpisodeOfCareBuilder episodeOfCareBuilder = csvHelper.getEpisodeOfCareCache().getEpisodeOfCareBuilder(parser);

        if (episodeOfCareBuilder != null) {

            csvHelper.setEpisodeReferenceOnEncounter(episodeOfCareBuilder, encounterBuilder, fhirResourceFiler);

            if (beginDate != null) {

                if (episodeOfCareBuilder.getRegistrationStartDate() == null || beginDate.before(episodeOfCareBuilder.getRegistrationStartDate())) {
                    episodeOfCareBuilder.setRegistrationStartDate(beginDate, beginDateCell);
                    episodeOfCareBuilder.setStatus(EpisodeOfCare.EpisodeOfCareStatus.ACTIVE);
                }

                // End date
                if (endDate != null) {
                    if (episodeOfCareBuilder.getRegistrationEndDate() == null || endDate.after(episodeOfCareBuilder.getRegistrationEndDate())) {
                        episodeOfCareBuilder.setRegistrationEndDate(endDate, endDateCell);
                    }
                }
            } else {
                if (episodeOfCareBuilder.getRegistrationEndDate() == null) {
                    episodeOfCareBuilder.setStatus(EpisodeOfCare.EpisodeOfCareStatus.PLANNED);
                }
            }
        }

        //we don't save immediately, but return the Encounter builder to the cache
        csvHelper.getEncounterCache().returnEncounterBuilder(encounterIdCell, encounterBuilder);
    }

    /**
     * creates a duplicate of the Encounter so that when we update it to be an Inpatient encounter, there
     * exists a FHIR Encounter recording the A&E attendance
     */
    private static void createEmegencyEncounterResourceCopy(EncounterBuilder oldEncounterBuilder, CsvCell encounterIdCell, BartsCsvHelper csvHelper) throws Exception {
        Encounter oldEncounter = (Encounter)oldEncounterBuilder.getResource();
        String json = FhirSerializationHelper.serializeResource(oldEncounter);
        Encounter newEncounter = (Encounter)FhirSerializationHelper.deserializeResource(json);
        EncounterBuilder newEncounterBuilder = new EncounterBuilder(newEncounter);

        //need to generate a new ID for this encounter using a special suffix
        String id = encounterIdCell.getString() + EncounterResourceCache.DUPLICATE_EMERGENCCY_ENCOUNTER_SUFFIX;

        //if ID Mapped already we need to forward map this new ID to a Discovery UUID
        if (newEncounterBuilder.isIdMapped()) {
            Reference encounterReference = ReferenceHelper.createReference(ResourceType.Encounter, id);
            encounterReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(encounterReference, csvHelper);
            id = ReferenceHelper.getReferenceId(encounterReference);
        }

        newEncounterBuilder.setId(id);

        //and add to the Encounter cache for later saving
        csvHelper.getEncounterCache().returnDuplicateEmergencyEncounterBuilder(encounterIdCell, newEncounterBuilder);

        //clear down any location, status or period data from the original Encounter, since that related to the A&E attendance portion
        //of the ENCNT and has been carried over onto the copy of the resource
        oldEncounter.getLocation().clear();
        oldEncounter.setStatus(null);
        oldEncounter.getStatusHistory().clear();
        oldEncounter.setPeriod(null);
    }
}
