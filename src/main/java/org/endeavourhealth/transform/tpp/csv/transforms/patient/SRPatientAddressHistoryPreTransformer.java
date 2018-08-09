package org.endeavourhealth.transform.tpp.csv.transforms.patient;

import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.patient.SRPatientAddressHistory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SRPatientAddressHistoryPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRPatientAddressHistoryTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        try {
            AbstractCsvParser parser = parsers.get(SRPatientAddressHistory.class);
            if (parser != null) {
                while (parser.nextRecord()) {

                    try {
                        processRecord((SRPatientAddressHistory) parser, fhirResourceFiler, csvHelper);
                    } catch (Exception ex) {
                        fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                    }
                }
            }
        } finally {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void processRecord(SRPatientAddressHistory parser, FhirResourceFiler fhirResourceFiler, TppCsvHelper csvHelper) throws Exception {

        CsvCell rowIdCell = parser.getRowIdentifier();
        CsvCell patientIdCell = parser.getIDPatient();
        if (patientIdCell.isEmpty()) {
            return;
        }

        Task task = new Task(parser.getCurrentState(), rowIdCell, patientIdCell, csvHelper);
        csvHelper.submitToThreadPool(task);
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
                csvHelper.saveInternalId(SRPatientAddressHistoryTransformer.ADDRESS_ID_TO_PATIENT_ID, rowIdCell.getString(), patientIdCell.getString());

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }
}
