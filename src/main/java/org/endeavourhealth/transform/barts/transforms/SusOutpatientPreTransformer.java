package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.core.database.dal.publisherStaging.models.*;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.SusOutpatient;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.TransformConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class SusOutpatientPreTransformer extends CdsPreTransformerBase {
    private static final Logger LOG = LoggerFactory.getLogger(SusOutpatientPreTransformer.class);

    //private static StagingCdsDalI repository = DalProvider.factoryStagingCdsDalI();

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        List<StagingProcedureCds> procedureBatch = new ArrayList<>();
        List<StagingProcedureCdsCount> procedureCountBatch = new ArrayList<>();
        List<StagingConditionCds> conditionBatch = new ArrayList<>();
        List<StagingConditionCdsCount> conditionCountBatch = new ArrayList<>();

        List<StagingOutpatientCds> outpatientCdsBatch = new ArrayList<>();

        for (ParserI parser : parsers) {

            while (parser.nextRecord()) {
                //no try/catch here, since any failure here means we don't want to continue

                //if Cerner transform config has been set with CDS Encounters only
                if (!TransformConfig.instance().isCernerCDSEncountersOnly()) {
                    processRecords(
                            (SusOutpatient) parser,
                            csvHelper,
                            BartsCsvHelper.SUS_RECORD_TYPE_OUTPATIENT,
                            procedureBatch,
                            procedureCountBatch,
                            conditionBatch,
                            conditionCountBatch);
                }

                //new function to call into Outpatient Encounters
                processOutpatientRecords((SusOutpatient)parser, csvHelper, outpatientCdsBatch);
            }
        }

        saveProcedureBatch(procedureBatch, true, csvHelper);
        saveProcedureCountBatch(procedureCountBatch, true, csvHelper);
        saveConditionBatch(conditionBatch, true, csvHelper);
        saveConditionCountBatch(conditionCountBatch, true, csvHelper);

        //new function to call into Outpatient attendances
        saveOutpatientCdsBatch(outpatientCdsBatch, true, csvHelper);
    }
}
