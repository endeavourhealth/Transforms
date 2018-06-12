package org.endeavourhealth.transform.adastra.csv.transforms;

import org.endeavourhealth.transform.adastra.AdastraCsvHelper;
import org.endeavourhealth.transform.adastra.cache.EpisodeOfCareResourceCache;
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
    }

    public static void createResource(OUTCOMES parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      AdastraCsvHelper csvHelper,
                                      String version) throws Exception {

        CsvCell caseId = parser.getCaseId();

        //get EpisodeofCare already populated from preceeding CASE transform
        EpisodeOfCareBuilder episodeBuilder
                = EpisodeOfCareResourceCache.getOrCreateEpisodeOfCareBuilder(caseId, csvHelper, fhirResourceFiler);

        //simple free text outcomes selected from a list, i.e. Prescription Given, Advice given - added as outcome extension
        CsvCell outcomeName = parser.getOutcomeName();
        if (!outcomeName.isEmpty()) {
            episodeBuilder.setOutcome(outcomeName.getString(), outcomeName);
        }
    }
}
