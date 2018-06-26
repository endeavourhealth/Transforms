package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.PPREL;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.CsvCurrentState;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Callable;

public class PPRELPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PPRELPreTransformer.class);

    public static final String PPREL_ID_TO_PERSON_ID = "PPREL_ID_TO_PERSON_ID";

    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        //we need to write a lot of stuff to the DB and each record is independent, so use a thread pool to parallelise
        int threadPoolSize = ConnectionManager.getPublisherCommonConnectionPoolMaxSize();
        ThreadPool threadPool = new ThreadPool(threadPoolSize, 10000);

        try {
            for (ParserI parser: parsers) {
                while (parser.nextRecord()) {

                    //no try/catch as failures here meant we should abort
                    processRecord((PPREL)parser, fhirResourceFiler, csvHelper, threadPool);
                }
            }
        } finally {
            List<ThreadPoolError> errors = threadPool.waitAndStop();
            handleErrors(errors);
        }
    }

    private static void handleErrors(List<ThreadPoolError> errors) throws Exception {
        if (errors == null || errors.isEmpty()) {
            return;
        }

        //if we've had multiple errors, just throw the first one, since they'll most-likely be the same
        ThreadPoolError first = errors.get(0);
        Throwable exception = first.getException();
        PPRELPreTransformCallable callable = (PPRELPreTransformCallable)first.getCallable();
        CsvCurrentState parserState = callable.getParserState();
        throw new TransformException(parserState.toString(), exception);
    }


    public static void processRecord(PPREL parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper, ThreadPool threadPool) throws Exception {

        //if non-active (i.e. deleted) we should REMOVE the identifier, but we don't get any other fields, including the Person ID
        //so we need to look it up via the internal ID mapping will have stored when we first created the identifier
        CsvCell active = parser.getActiveIndicator();
        if (!active.getIntAsBoolean()) {
            return;
        }

        //we need to store a mapping of alias ID to person ID
        CsvCell relatedPersonIdCell = parser.getRelatedPersonMillenniumIdentifier();
        CsvCell personIdCell = parser.getMillenniumPersonIdentifier();

        PPRELPreTransformCallable callable = new PPRELPreTransformCallable(parser.getCurrentState(), relatedPersonIdCell, personIdCell, csvHelper);
        List<ThreadPoolError> errors = threadPool.submit(callable);
        handleErrors(errors);
    }


    static class PPRELPreTransformCallable implements Callable {

        private CsvCurrentState parserState = null;
        private CsvCell relatedPersonIdCell = null;
        private CsvCell personIdCell = null;
        private BartsCsvHelper csvHelper = null;

        public PPRELPreTransformCallable(CsvCurrentState parserState,
                                         CsvCell relatedPersonIdCell,
                                         CsvCell personIdCell,
                                         BartsCsvHelper csvHelper) {

            this.parserState = parserState;
            this.relatedPersonIdCell = relatedPersonIdCell;
            this.personIdCell = personIdCell;
            this.csvHelper = csvHelper;
        }

        @Override
        public Object call() throws Exception {

            try {

                //we need to store the PPREL ID -> PERSON ID mapping so that if the address is ever deleted,
                //we can find the person it belonged to, since the deleted records only give us the ID
                csvHelper.saveInternalId(PPREL_ID_TO_PERSON_ID, relatedPersonIdCell.getString(), personIdCell.getString());

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }

        public CsvCurrentState getParserState() {
            return parserState;
        }
    }
}


