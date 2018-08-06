package org.endeavourhealth.transform.tpp.csv.transforms.patient;

import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.patient.SRPatientContactDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class SRPatientContactDetailsPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRPatientContactDetailsPreTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {


        //we're just streaming content, row by row, into the DB, so use a threadpool to parallelise it
        int threadPoolSize = ConnectionManager.getPublisherCommonConnectionPoolMaxSize();
        ThreadPool threadPool = new ThreadPool(threadPoolSize, 10000);

        try {
            AbstractCsvParser parser = parsers.get(SRPatientContactDetails.class);
            if (parser != null) {
                while (parser.nextRecord()) {

                    try {
                        processRecord((SRPatientContactDetails) parser, fhirResourceFiler, csvHelper, threadPool);
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

    private static void processRecord(SRPatientContactDetails parser, FhirResourceFiler fhirResourceFiler, TppCsvHelper csvHelper, ThreadPool threadPool) throws Exception {

        CsvCell rowIdCell = parser.getRowIdentifier();
        CsvCell patientIdCell = parser.getIDPatient();
        if (patientIdCell.isEmpty()) {
            return;
        }

        Task task = new Task(parser.getCurrentState(), rowIdCell, patientIdCell, csvHelper);
        List<ThreadPoolError> errors = threadPool.submit(task);
        AbstractCsvCallable.handleErrors(errors);
    }

    static class Task extends AbstractCsvCallable {

        private CsvCell rowIdCell;
        private CsvCell patientIdCell;
        private TppCsvHelper csvHelper;

        public Task(CsvCurrentState parserState, CsvCell rowIdCell, CsvCell patientIdCell, TppCsvHelper csvHelper) {
            super(parserState);
            this.rowIdCell = rowIdCell;
            this.patientIdCell = patientIdCell;
            this.csvHelper = csvHelper;
        }

        @Override
        public Object call() throws Exception {

            try {
                csvHelper.saveInternalId(SRPatientContactDetailsTransformer.PHONE_ID_TO_PATIENT_ID, rowIdCell.getString(), patientIdCell.getString());

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }
}

