package org.endeavourhealth.transform.adastra.csv.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.transform.adastra.AdastraCsvHelper;
import org.endeavourhealth.transform.adastra.csv.schema.OUTCOMES;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class OUTCOMESTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(OUTCOMESTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 AdastraCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(OUTCOMES.class);

        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((OUTCOMES) parser, fhirResourceFiler, csvHelper, version);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createResource(OUTCOMES parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      AdastraCsvHelper csvHelper,
                                      String version) throws Exception {

        CsvCell caseId = parser.getCaseId();

        //get EpisodeofCare already populated from preceeding CASE transform
        EpisodeOfCareBuilder episodeBuilder
                = csvHelper.getEpisodeOfCareCache().getOrCreateEpisodeOfCareBuilder(caseId, csvHelper, fhirResourceFiler);

        //simple free text outcomes selected from a list, i.e. Prescription Given, Advice given - added and appended to an outcome extension
        CsvCell outcomeName = parser.getOutcomeName();
        if (!outcomeName.isEmpty()) {

            String outcomeText = outcomeName.getString();

            // get existing outcome text to update, can have multiple outcome lines
            String existingOutcomeText = csvHelper.getCaseOutcome(caseId.getString());
            if (!Strings.isNullOrEmpty(existingOutcomeText)) {

                outcomeText = existingOutcomeText.concat(", ").concat(outcomeText);
            }

            episodeBuilder.setOutcome(outcomeText, outcomeName);

            //cache the new episode outcome
            csvHelper.cacheCaseOutcome(caseId.getString(), outcomeText);

        }

        // return the builder back to the cache
        csvHelper.getEpisodeOfCareCache().returnEpisodeOfCareBuilder(caseId, episodeBuilder);
    }
}