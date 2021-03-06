package org.endeavourhealth.transform.homerton.transforms;

import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.homerton.HomertonCsvHelper;
import org.endeavourhealth.transform.homerton.schema.ProcedureTable;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ProcedurePreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ProcedurePreTransformer.class);


    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 HomertonCsvHelper csvHelper) throws Exception {

        //we need to write a lot of stuff to the DB and each record is independent, so use a thread pool to parallelise
        int threadPoolSize = ConnectionManager.getPublisherCommonConnectionPoolMaxSize();
        ThreadPool threadPool = new ThreadPool(threadPoolSize, 1000, "HomertonProcedure");

        try {
            for (ParserI parser: parsers) {
                while (parser.nextRecord()) {
                    //no try/catch here, since any failure here means we don't want to continue
                    processRecord((ProcedureTable)parser, fhirResourceFiler, csvHelper, threadPool);
                }
            }

        } finally {
            List<ThreadPoolError> errors = threadPool.waitAndStop();
            AbstractCsvCallable.handleErrors(errors);
        }

    }

    public static void processRecord(ProcedureTable parser, FhirResourceFiler fhirResourceFiler, HomertonCsvHelper csvHelper, ThreadPool threadPool) throws Exception {

        CsvCell procedureIdCell = parser.getProcedureID();
        CsvCell encounterIdCell = parser.getEncounterID();

        PreTransformCallable callable = new PreTransformCallable(parser.getCurrentState(), procedureIdCell, encounterIdCell, csvHelper);
        List<ThreadPoolError> errors = threadPool.submit(callable);
        AbstractCsvCallable.handleErrors(errors);

    }


    static class PreTransformCallable extends AbstractCsvCallable {

        private CsvCell procedureIdCell;
        private CsvCell encounterIdCell;
        private HomertonCsvHelper csvHelper;

        public PreTransformCallable(CsvCurrentState parserState, CsvCell procedureIdCell, CsvCell encounterIdCell, HomertonCsvHelper csvHelper) {
            super(parserState);
            this.procedureIdCell = procedureIdCell;
            this.encounterIdCell = encounterIdCell;
            this.csvHelper = csvHelper;
        }


        @Override
        public Object call() throws Exception {

            try {

                //LOG.debug("Caching ProcedureId {} against EncounterId {}", procedureIdCell.getString(), encounterIdCell.getString());
                csvHelper.cacheNewConsultationChildRelationship(encounterIdCell, procedureIdCell, ResourceType.Procedure);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }
}


