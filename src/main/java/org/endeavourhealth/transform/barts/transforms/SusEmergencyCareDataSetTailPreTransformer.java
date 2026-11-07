package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingCdsTail;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.SusEmergencyCareDataSetTail;
import org.endeavourhealth.transform.barts.schema.SusEmergencyCareDataSetTailV2;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class SusEmergencyCareDataSetTailPreTransformer extends CdsTailPreTransformerBase {
    private static final Logger LOG = LoggerFactory.getLogger(SusEmergencyCareDataSetTailPreTransformer.class);


    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        List<StagingCdsTail> cdsTailBatch = new ArrayList<>();

        for (ParserI parser : parsers) {

            while (parser.nextRecord()) {
                //no try/catch here, since any failure here means we don't want to continue

                processEmergencyCdsTailRecords((SusEmergencyCareDataSetTail)parser, csvHelper, cdsTailBatch);
            }
        }

        saveCdsTailBatch(cdsTailBatch, true, csvHelper);
    }

    public static void transformV2(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        List<StagingCdsTail> cdsTailBatch = new ArrayList<>();

        for (ParserI parser : parsers) {

            while (parser.nextRecord()) {
                //no try/catch here, since any failure here means we don't want to continue

                processEmergencyCdsTailRecords((SusEmergencyCareDataSetTailV2)parser, csvHelper, cdsTailBatch);
            }
        }

        saveCdsTailBatch(cdsTailBatch, true, csvHelper);
    }
}
