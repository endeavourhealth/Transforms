package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.PPPHO;
import org.endeavourhealth.transform.common.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PPPHOPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPPHOPreTransformer.class);

    public static final String PPPHO_ID_TO_PERSON_ID = "PPPHO_ID_TO_PERSON_ID";

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        //we need to write a lot of stuff to the DB and each record is independent, so use a thread pool to parallelise
        int threadPoolSize = ConnectionManager.getPublisherCommonConnectionPoolMaxSize();
        ThreadPool threadPool = new ThreadPool(threadPoolSize, 10000);

        try {
            for (ParserI parser: parsers) {
                while (parser.nextRecord()) {

                    if (!csvHelper.processRecordFilteringOnPatientId((AbstractCsvParser)parser)) {
                        continue;
                    }

                    //no try/catch as failures here meant we should abort
                    processRecord((PPPHO)parser, fhirResourceFiler, csvHelper, threadPool);
                }
            }
        } finally {
            List<ThreadPoolError> errors = threadPool.waitAndStop();
            AbstractCsvCallable.handleErrors(errors);
        }
    }


    public static void processRecord(PPPHO parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper, ThreadPool threadPool) throws Exception {

        //if non-active (i.e. deleted) we should REMOVE the identifier, but we don't get any other fields, including the Person ID
        //so we need to look it up via the internal ID mapping will have stored when we first created the identifier
        CsvCell active = parser.getActiveIndicator();
        if (!active.getIntAsBoolean()) {
            return;
        }

        //we need to store a mapping of alias ID to person ID
        CsvCell phoneIdCell = parser.getMillenniumPhoneId();
        CsvCell personIdCell = parser.getMillenniumPersonIdentifier();

        PPPHOPreTransformCallable callable = new PPPHOPreTransformCallable(parser.getCurrentState(), phoneIdCell, personIdCell, csvHelper);
        List<ThreadPoolError> errors = threadPool.submit(callable);
        AbstractCsvCallable.handleErrors(errors);
    }


    static class PPPHOPreTransformCallable extends AbstractCsvCallable {

        private CsvCell phoneIdCell = null;
        private CsvCell personIdCell = null;
        private BartsCsvHelper csvHelper = null;

        public PPPHOPreTransformCallable(CsvCurrentState parserState,
                                         CsvCell phoneIdCell,
                                         CsvCell personIdCell,
                                         BartsCsvHelper csvHelper) {

            super(parserState);
            this.phoneIdCell = phoneIdCell;
            this.personIdCell = personIdCell;
            this.csvHelper = csvHelper;
        }

        @Override
        public Object call() throws Exception {

            try {

                //we need to store the PPPHO ID -> PERSON ID mapping so that if the address is ever deleted,
                //we can find the person it belonged to, since the deleted records only give us the ID
                csvHelper.saveInternalId(PPPHO_ID_TO_PERSON_ID, phoneIdCell.getString(), personIdCell.getString());

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }
}

