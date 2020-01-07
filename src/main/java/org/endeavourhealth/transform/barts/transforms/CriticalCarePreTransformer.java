package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingCriticalCareCds;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.CriticalCare;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class CriticalCarePreTransformer extends CdsPreTransformerBase {
    private static final Logger LOG = LoggerFactory.getLogger(CriticalCarePreTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        List<StagingCriticalCareCds> criticalCareCdsBatch = new ArrayList<>();

        for (ParserI parser : parsers) {

            while (parser.nextRecord()) {
                //no try/catch here, since any failure here means we don't want to continue

                //new function to call into emergency attendances
                processCriticalCareCdsRecords((CriticalCare)parser, csvHelper, criticalCareCdsBatch);
            }
        }

        saveCriticalCareCdsBatch(criticalCareCdsBatch, true, csvHelper);
    }
}
