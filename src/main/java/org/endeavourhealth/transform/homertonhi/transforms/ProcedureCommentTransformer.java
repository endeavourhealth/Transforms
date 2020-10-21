package org.endeavourhealth.transform.homertonhi.transforms;

import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.homertonhi.HomertonHiCsvHelper;
import org.endeavourhealth.transform.homertonhi.schema.ProcedureComment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ProcedureCommentTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(ProcedureCommentTransformer.class);

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 HomertonHiCsvHelper csvHelper) throws Exception {

           for (ParserI parser: parsers) {
                try {

                    while (parser.nextRecord()) {
                        //no try/catch here, since any failure here means we don't want to continue
                        processRecord((ProcedureComment) parser, csvHelper);
                    }
                } catch (Exception ex) {

                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }

            //call this to abort if we had any errors, during the above processing
            fhirResourceFiler.failIfAnyErrors();
    }

    public static void processRecord(ProcedureComment parser, HomertonHiCsvHelper csvHelper) throws Exception {

        CsvCell procedureIdCell = parser.getProcedureId();
        CsvCell procedureCommentTextCell = parser.getProcedureCommentText();

        //simply cache the procedure comments against the id for retrieval in the Procedure transform
        csvHelper.cacheProcedureCommentText(procedureIdCell, procedureCommentTextCell);
    }
}