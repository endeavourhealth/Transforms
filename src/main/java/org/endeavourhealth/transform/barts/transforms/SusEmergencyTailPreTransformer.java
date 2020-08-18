package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingCdsTail;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingConditionCdsTail;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingProcedureCdsTail;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.SusEmergencyTail;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class SusEmergencyTailPreTransformer extends CdsTailPreTransformerBase {
    private static final Logger LOG = LoggerFactory.getLogger(SusEmergencyTailPreTransformer.class);
    //private static StagingCdsTailDalI repository = DalProvider.factoryStagingCdsTailDalI();

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        List<StagingProcedureCdsTail> procedureBatch = new ArrayList<>();
        List<StagingConditionCdsTail> conditionBatch = new ArrayList<>();
        List<StagingCdsTail> cdsTailBatch = new ArrayList<>();

        for (ParserI parser : parsers) {

            while (parser.nextRecord()) {
                //no try/catch here, since any failure here means we don't want to continue
                processTailRecord((SusEmergencyTail)parser,
                        csvHelper,
                        BartsCsvHelper.SUS_RECORD_TYPE_EMERGENCY,
                        procedureBatch,
                        conditionBatch,
                        cdsTailBatch);
            }
        }

        saveProcedureBatch(procedureBatch, true, csvHelper);
        saveConditionBatch(conditionBatch, true, csvHelper);
        saveCdsTailBatch(cdsTailBatch, true, csvHelper);
    }
}


