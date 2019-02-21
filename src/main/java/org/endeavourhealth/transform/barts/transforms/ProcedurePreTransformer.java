package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.ProcedurePojo;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ProcedurePreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ProcedurePreTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {

            while (parser.nextRecord()) {
                try {
                    processRecord((org.endeavourhealth.transform.barts.schema.Procedure)parser, csvHelper);

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
        pojo.setEncounterId(parser.getEncounterId()); // Remember encounter ids from Procedure have a trailing .00
        pojo.setNotes(parser.getComment());
        pojo.setMrn(parser.getMrn());
        pojo.setProcedureCode(parser.getProcedureCode());
        csvHelper.getProcedureCache().cachePojo(pojo);

    }

}
