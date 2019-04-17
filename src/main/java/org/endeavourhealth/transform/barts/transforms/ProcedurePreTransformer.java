package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherStaging.StagingProcedureDalI;
import org.endeavourhealth.core.database.dal.publisherStaging.models.StagingProcedure;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.common.AbstractCsvCallable;
import org.endeavourhealth.transform.common.CsvCurrentState;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ProcedurePreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ProcedurePreTransformer.class);

    private static StagingProcedureDalI repository = DalProvider.factoryBartsStagingDataDalI();


    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser : parsers) {

            while (parser.nextRecord()) {
                try {
                    processRecord((org.endeavourhealth.transform.barts.schema.Procedure) parser, csvHelper);

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void processRecord(org.endeavourhealth.transform.barts.schema.Procedure parser, BartsCsvHelper csvHelper) throws Exception {


        StagingProcedure obj = new StagingProcedure();
        obj.setExchangeId(parser.getExchangeId().toString());

        obj.setConsultant(parser.getConsultant().getString());
        obj.setProc_dt_tm(parser.getProcedureDateTime().getDate());
        obj.setUpdatedBy(parser.getUpdatedBy().getInt());
        obj.setCreate_dt_tm(parser.getCreateDateTime().getDate());
        obj.setEncounterId(parser.getEncounterId().getInt()); // Remember encounter ids from Procedure have a trailing .00
        obj.setComments(parser.getComment().getString());
        obj.setPersonId(parser.getMrn().getInt());
        obj.setProcedureCode(parser.getProcedureCode().getString());

        csvHelper.submitToThreadPool(new ProcedurePreTransformer.saveDataCallable(parser.getCurrentState(), obj));
    }

    private static class saveDataCallable extends AbstractCsvCallable {

        private StagingProcedure obj = null;

        public saveDataCallable(CsvCurrentState parserState,
                                        StagingProcedure obj) {
            super(parserState);
            this.obj = obj;
        }

        @Override
        public Object call() throws Exception {

            try {
                repository.saveStagingProcedure(obj);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }


}
