package org.endeavourhealth.transform.homerton.transforms;

import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.homerton.HomertonCsvHelper;
import org.endeavourhealth.transform.homerton.schema.DiagnosisTable;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DiagnosisPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(DiagnosisPreTransformer.class);


    public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 HomertonCsvHelper csvHelper) throws Exception {

        //we need to write a lot of stuff to the DB and each record is independent, so use a thread pool to parallelise
        int threadPoolSize = ConnectionManager.getPublisherCommonConnectionPoolMaxSize();
        ThreadPool threadPool = new ThreadPool(threadPoolSize, 1000, "HomertonDiagnosis");

        try {
            for (ParserI parser: parsers) {
                while (parser.nextRecord()) {

                    //no try/catch here, since any failure here means we don't want to continue
                    processRecord((DiagnosisTable)parser, fhirResourceFiler, csvHelper, threadPool);
                }
            }

        } finally {
            List<ThreadPoolError> errors = threadPool.waitAndStop();
            AbstractCsvCallable.handleErrors(errors);
        }
    }

    public static void processRecord(DiagnosisTable parser, FhirResourceFiler fhirResourceFiler, HomertonCsvHelper csvHelper, ThreadPool threadPool) throws Exception {

        CsvCell diagnosisIdCell = parser.getDiagnosisID();
        CsvCell encounterIdCell = parser.getEncounterID();

        PreTransformCallable callable = new PreTransformCallable(parser.getCurrentState(), diagnosisIdCell, encounterIdCell, csvHelper);
        List<ThreadPoolError> errors = threadPool.submit(callable);
        AbstractCsvCallable.handleErrors(errors);
    }


    static class PreTransformCallable extends AbstractCsvCallable {

        private CsvCell diagnosisIdCell;
        private CsvCell encounterIdCell;
        private HomertonCsvHelper csvHelper;

        public PreTransformCallable(CsvCurrentState parserState, CsvCell diagnosisIdCell, CsvCell encounterIdCell, HomertonCsvHelper csvHelper) {
            super(parserState);
            this.diagnosisIdCell = diagnosisIdCell;
            this.encounterIdCell = encounterIdCell;
            this.csvHelper = csvHelper;
        }


        @Override
        public Object call() throws Exception {

            try {

                //LOG.debug("Caching DiagnosisId {} against EncounterId {}", diagnosisIdCell.getString(), encounterIdCell.getString());
                csvHelper.cacheNewConsultationChildRelationship(encounterIdCell, diagnosisIdCell, ResourceType.Condition);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }

}
