package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherTransform.BartsStagingDataDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.BartsStagingDataProcedure;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.cache.ProcedurePojo;
import org.endeavourhealth.transform.common.AbstractCsvCallable;
import org.endeavourhealth.transform.common.CsvCurrentState;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ProcedurePreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ProcedurePreTransformer.class);

    private static BartsStagingDataDalI repository = DalProvider.factoryBartsStagingDataDalI();


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

        ProcedurePojo pojo = new ProcedurePojo();
        pojo.setConsultant(parser.getConsultant());
        pojo.setProc_dt_tm(parser.getProcedureDateTime());
        pojo.setUpdatedBy(parser.getUpdatedBy());
        pojo.setCreate_dt_tm(parser.getCreateDateTime());
        pojo.setEncounterId(parser.getEncounterId()); // Remember encounter ids from Procedure have a trailing .00
        pojo.setNotes(parser.getComment());
        pojo.setMrn(parser.getMrn());
        pojo.setProcedureCode(parser.getProcedureCode());
        BartsStagingDataProcedure obj = toBartsStagingData(pojo);

        csvHelper.submitToThreadPool(new ProcedurePreTransformer.saveDataCallable(parser.getCurrentState(), obj));
    }

    private static class saveDataCallable extends AbstractCsvCallable {

        private BartsStagingDataProcedure obj = null;

        public saveDataCallable(CsvCurrentState parserState,
                                        BartsStagingDataProcedure obj) {
            super(parserState);
            this.obj = obj;
        }

        @Override
        public Object call() throws Exception {

            try {
                repository.saveBartsStagingDataProcedure(obj);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }
    private static BartsStagingDataProcedure toBartsStagingData(ProcedurePojo in) throws  Exception{

        BartsStagingDataProcedure ret = new BartsStagingDataProcedure();
        ret.setExchangeId(in.getExchangeId());
        ret.setEncounterId(in.getEncounterId().getInt());
        ret.setPersonId(in.getMRN().getInt());
        ret.setWard(in.getWard().getString());
        ret.setSite(in.getSite().getString());
        ret.setConsultant(in.getConsultant().getString());
        ret.setProc_dt_tm(in.getProc_dt_tm().getDate());
        ret.setCreate_dt_tm(in.getCreate_dt_tm().getDate());
        ret.setUpdatedBy(in.getUpdatedBy().getInt());
        ret.setNotes(in.getNotes().getString());
        ret.setProcedureCode(in.getProcedureCode().getString());
        ret.setProcedureCodeType(in.getProcedureCodeType().getString());
        ret.setComparisonCode(in.getComparisonCode());

        return ret;

    }

}
