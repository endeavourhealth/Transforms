package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.PROCE;
import org.endeavourhealth.transform.common.*;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class PROCEPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PROCEPreTransformer.class);


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
                    //no try/catch here, since any failure here means we don't want to continue
                    processRecord((PROCE)parser, fhirResourceFiler, csvHelper, threadPool);
                }
            }

        } finally {
            List<ThreadPoolError> errors = threadPool.waitAndStop();
            AbstractCsvCallable.handleErrors(errors);
        }

    }

    public static void processRecord(PROCE parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper, ThreadPool threadPool) throws Exception {

        CsvCell procedureIdCell = parser.getProcedureID();
        CsvCell encounterIdCell = parser.getEncounterId();


        PreTransformCallable callable = new PreTransformCallable(parser.getCurrentState(), procedureIdCell, encounterIdCell, csvHelper);
        List<ThreadPoolError> errors = threadPool.submit(callable);
        AbstractCsvCallable.handleErrors(errors);

    }


    static class PreTransformCallable extends AbstractCsvCallable {

        private CsvCell procedureIdCell;
        private CsvCell encounterIdCell;
        private BartsCsvHelper csvHelper;

        public PreTransformCallable(CsvCurrentState parserState, CsvCell procedureIdCell, CsvCell encounterIdCell, BartsCsvHelper csvHelper) {
            super(parserState);
            this.procedureIdCell = procedureIdCell;
            this.encounterIdCell = encounterIdCell;
            this.csvHelper = csvHelper;
        }


        @Override
        public Object call() throws Exception {

            try {
                csvHelper.cacheNewConsultationChildRelationship(encounterIdCell, procedureIdCell, ResourceType.Procedure);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }
}


