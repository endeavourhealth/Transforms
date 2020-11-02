package org.endeavourhealth.transform.homertonhi.transforms;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.*;
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

        try {
            for (ParserI parser : parsers) {
                try {
                    while (parser.nextRecord()) {

                        processRecord((ProcedureComment) parser, csvHelper);
                    }
                } catch (Exception ex) {

                    throw new TransformException(parser.getCurrentState().toString(), ex);
                }
            }
        } finally {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void processRecord(ProcedureComment parser, HomertonHiCsvHelper csvHelper) throws Exception {

        CsvCell procedureIdCell = parser.getProcedureId();
        CsvCell procedureCommentTextCell = parser.getProcedureCommentText();

        CsvCurrentState parserState = parser.getCurrentState();

        ProcedureCommentTransformer.PreTransformTask task
                = new ProcedureCommentTransformer.PreTransformTask(parserState, procedureIdCell, procedureCommentTextCell, csvHelper);
        csvHelper.submitToThreadPool(task);
    }

    static class PreTransformTask extends AbstractCsvCallable {

        private CsvCell procedureIdCell;
        private CsvCell procedureCommentTextCell;
        private HomertonHiCsvHelper csvHelper;

        public PreTransformTask(CsvCurrentState parserState,
                          CsvCell procedureIdCell,
                          CsvCell procedureCommentTextCell,
                          HomertonHiCsvHelper csvHelper) {

            super(parserState);
            this.procedureIdCell = procedureIdCell;
            this.procedureCommentTextCell = procedureCommentTextCell;
            this.csvHelper = csvHelper;
        }

        @Override
        public Object call() throws Exception {
            try {

                //simply cache the procedure comments against the id for retrieval in the Procedure transform
                csvHelper.cacheProcedureCommentText(procedureIdCell, procedureCommentTextCell);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }
}