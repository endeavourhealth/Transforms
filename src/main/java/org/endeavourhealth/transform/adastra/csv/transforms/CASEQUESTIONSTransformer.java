package org.endeavourhealth.transform.adastra.csv.transforms;

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

        //get EpisodeofCare already populated from preceeding CASE transform
        EpisodeOfCareBuilder episodeBuilder
                = EpisodeOfCareResourceCache.getOrCreateEpisodeOfCareBuilder(caseId, csvHelper, fhirResourceFiler);

        //TODO - where set Case questions?

    }
}
