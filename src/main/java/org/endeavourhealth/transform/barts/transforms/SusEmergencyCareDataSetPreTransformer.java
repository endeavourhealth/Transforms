package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingEmergencyCds;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.SusEmergencyCareDataSet;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class SusEmergencyCareDataSetPreTransformer extends CdsPreTransformerBase {
    private static final Logger LOG = LoggerFactory.getLogger(SusEmergencyCareDataSetPreTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        List<StagingEmergencyCds> emergencyCdsBatch = new ArrayList<>();

        for (ParserI parser : parsers) {

            while (parser.nextRecord()) {
                //no try/catch here, since any failure here means we don't want to continue

                //new function to call into emergency Encounters
                processEmergencyCdsRecords((SusEmergencyCareDataSet)parser, csvHelper, emergencyCdsBatch);
            }
        }

        saveEmergencyCdsBatch(emergencyCdsBatch, true, csvHelper);
    }
}
