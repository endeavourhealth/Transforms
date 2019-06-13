package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingConditionCds;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingConditionCdsCount;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingProcedureCds;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingProcedureCdsCount;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.SusEmergency;
import org.endeavourhealth.transform.barts.schema.SusInpatient;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class SusInpatientPreTransformer extends CdsPreTransformerBase {
    private static final Logger LOG = LoggerFactory.getLogger(SusInpatientPreTransformer.class);

    //private static StagingCdsDalI repository = DalProvider.factoryStagingCdsDalI();

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        List<StagingProcedureCds> procedureBatch = new ArrayList<>();
        List<StagingProcedureCdsCount> procedureCountBatch = new ArrayList<>();
        List<StagingConditionCds> conditionBatch = new ArrayList<>();
        List<StagingConditionCdsCount> conditionCountBatch = new ArrayList<>();

        for (ParserI parser : parsers) {

            while (parser.nextRecord()) {
                //no try/catch here, since any failure here means we don't want to continue
                processRecords((SusInpatient)parser, csvHelper, BartsCsvHelper.SUS_RECORD_TYPE_INPATIENT, procedureBatch, procedureCountBatch, conditionBatch, conditionCountBatch);
            }
        }

        saveProcedureBatch(procedureBatch, true, csvHelper);
        saveProcedureCountBatch(procedureCountBatch, true, csvHelper);
        saveConditionBatch(conditionBatch, true, csvHelper);
        saveConditionCountBatch(conditionCountBatch, true, csvHelper);
    }

}
