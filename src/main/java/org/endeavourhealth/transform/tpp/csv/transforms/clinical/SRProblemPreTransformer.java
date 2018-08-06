package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SRProblem;
import org.endeavourhealth.transform.tpp.csv.transforms.patient.SRPatientAddressHistoryTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class SRProblemPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRPatientAddressHistoryTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {


        //we're just streaming content, row by row, into the DB, so use a threadpool to parallelise it
        int threadPoolSize = ConnectionManager.getPublisherCommonConnectionPoolMaxSize();
        ThreadPool threadPool = new ThreadPool(threadPoolSize, 10000);

        try {
            AbstractCsvParser parser = parsers.get(SRProblem.class);
            if (parser != null) {
                while (parser.nextRecord()) {

                    try {
                        processRecord((SRProblem) parser, fhirResourceFiler, csvHelper, threadPool);
                    } catch (Exception ex) {
                        fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                    }
                }
            }
        } finally {
            List<ThreadPoolError> errors = threadPool.waitAndStop();
            AbstractCsvCallable.handleErrors(errors);
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void processRecord(SRProblem parser, FhirResourceFiler fhirResourceFiler, TppCsvHelper csvHelper, ThreadPool threadPool) throws Exception {

        CsvCell problemIdCell = parser.getRowIdentifier();
        CsvCell codeIdCell = parser.getIDCode();
        if (codeIdCell.isEmpty()) { //this will happen if the record is a delete
            return;
        }

        Task task = new Task(parser.getCurrentState(), problemIdCell, codeIdCell, csvHelper);
        List<ThreadPoolError> errors = threadPool.submit(task);
        AbstractCsvCallable.handleErrors(errors);
    }

    static class Task extends AbstractCsvCallable {

        private CsvCell problemIdCell;
        private CsvCell codeIdCell;
        private TppCsvHelper csvHelper;

        public Task(CsvCurrentState parserState, CsvCell problemIdCell, CsvCell codeIdCell, TppCsvHelper csvHelper) {
            super(parserState);
            this.problemIdCell = problemIdCell;
            this.codeIdCell = codeIdCell;
            this.csvHelper = csvHelper;
        }

        @Override
        public Object call() throws Exception {

            try {
                csvHelper.saveInternalId(SRProblemTransformer.PROBLEM_ID_TO_CODE_ID, problemIdCell.getString(), codeIdCell.getString());

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }
}
