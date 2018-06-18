package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.cache.EncounterResourceCache;
import org.endeavourhealth.transform.barts.cache.EpisodeOfCareResourceCache;
import org.endeavourhealth.transform.barts.schema.IPEPI;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.EpisodeOfCare;
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
                try {
                    createEpisodeEvent((IPEPI) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }

    public static void createEpisodeEvent(IPEPI parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        CsvCell encounterIdCell = parser.getEncounterId();
        CsvCell activeCell = parser.getActiveIndicator();
        CsvCell personIdCell = parser.getPatientId();
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

        // get the associated encounter
        EncounterBuilder encounterBuilder = EncounterResourceCache.getEncounterBuilder(encounterIdCell, personIdCell, activeCell, csvHelper);

        if (!activeCell.getIntAsBoolean()) {
            EncounterResourceCache.deleteEncounter(encounterBuilder, encounterIdCell, fhirResourceFiler, parser.getCurrentState());
            return;
        }

        encounterBuilder.setClass(Encounter.EncounterClass.INPATIENT);

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
        EpisodeOfCareBuilder episodeOfCareBuilder = EpisodeOfCareResourceCache.getEpisodeOfCareBuilder(null, encounterIdCell, personIdCell, activeCell, csvHelper);

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

        //no need to save anything, as the Encounter and Episode caches sort that out later
    }
}
