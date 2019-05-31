package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.SusOutpatient;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SusOutpatientPreTransformer extends CdsPreTransformerBase {
    private static final Logger LOG = LoggerFactory.getLogger(SusOutpatientPreTransformer.class);

    //private static StagingCdsDalI repository = DalProvider.factoryStagingCdsDalI();

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {
        for (ParserI parser : parsers) {

            while (parser.nextRecord()) {
                //no try/catch here, since any failure here means we don't want to continue
                processRecords((SusOutpatient)parser, csvHelper, BartsCsvHelper.SUS_RECORD_TYPE_OUTPATIENT);
            }
        }
    }


}
