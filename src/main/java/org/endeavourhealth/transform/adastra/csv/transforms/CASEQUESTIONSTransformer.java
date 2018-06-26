package org.endeavourhealth.transform.adastra.csv.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.transform.adastra.AdastraCsvHelper;
import org.endeavourhealth.transform.adastra.cache.EpisodeOfCareResourceCache;
import org.endeavourhealth.transform.adastra.csv.schema.CASEQUESTIONS;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CASEQUESTIONSTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(CASEQUESTIONSTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 AdastraCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(CASEQUESTIONS.class);

        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((CASEQUESTIONS) parser, fhirResourceFiler, csvHelper, version);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }

    public static void createResource(CASEQUESTIONS parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      AdastraCsvHelper csvHelper,
                                      String version) throws Exception {

        CsvCell caseId = parser.getCaseId();

        EpisodeOfCareBuilder episodeBuilder
                = EpisodeOfCareResourceCache.getOrCreateEpisodeOfCareBuilder(caseId, csvHelper, fhirResourceFiler);

        CsvCell questionSetName = parser.getQuestionSetName();
        CsvCell question = parser.getQuestion();

        // Outcomes are handled in the Outcomes transformer.
        // Capture non Outcomes text here, i.e. safe guarding and additional comments
        if (!questionSetName.getString().toLowerCase().contains("outcomes") ||
                question.getString().toLowerCase().contains("additional")) {

            CsvCell answerOutcome = parser.getAnswer();
            if (!answerOutcome.isEmpty()) {

                String answerOutcomeText = "";
                //append the question set to the answer to give it some context
                if (!question.getString().toLowerCase().contains("additional")) {

                    answerOutcomeText = questionSetName.getString().concat(": ").concat(answerOutcome.getString());
                } else {
                    answerOutcomeText = question.getString().concat(": ").concat(answerOutcome.getString());
                }

                //get existing outcome text to update
                String existingOutcomeText = csvHelper.getCaseOutcome(caseId.getString());
                if (!Strings.isNullOrEmpty(existingOutcomeText)) {

                    answerOutcomeText = existingOutcomeText.concat(", ").concat(answerOutcomeText);
                }

                episodeBuilder.setOutcome(answerOutcomeText, answerOutcome);

                //cache the new episode outcome
                csvHelper.cacheCaseOutcome(caseId.getString(), answerOutcomeText);
            }
        }
    }
}
