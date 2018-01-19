package org.endeavourhealth.transform.emis.csv.transforms.careRecord;

import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.transform.common.CsvCurrentState;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.exceptions.TransformException;
import org.endeavourhealth.transform.emis.csv.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.schema.AbstractCsvParser;
import org.endeavourhealth.transform.emis.csv.schema.careRecord.Consultation;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class ConsultationPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ConsultationPreTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        //use a thread pool to perform multiple lookups in parallel
        int threadPoolSize = ConnectionManager.getPublisherTransformConnectionPoolMaxSize(fhirResourceFiler.getServiceId());
        ThreadPool threadPool = new ThreadPool(threadPoolSize, 50000);

        //unlike most of the other parsers, we don't handle record-level exceptions and continue, since a failure
        //to parse any record in this file it a critical error
        try {
            AbstractCsvParser parser = parsers.get(Consultation.class);
            while (parser.nextRecord()) {

                try {
                    transform((Consultation)parser, fhirResourceFiler, csvHelper, threadPool);
                } catch (Exception ex) {
                    throw new TransformException(parser.getCurrentState().toString(), ex);
                }
            }

        } finally {
            List<ThreadPoolError> errors = threadPool.waitAndStop();
            handleErrors(errors);
        }
    }

    public static void transform(Consultation parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      EmisCsvHelper csvHelper,
                                      ThreadPool threadPool) throws Exception {

        String consultationGuid = parser.getConsultationGuid();
        String patientGuid = parser.getPatientGuid();

        String encounterSourceId = EmisCsvHelper.createUniqueId(patientGuid, consultationGuid);

        CsvCurrentState parserState = parser.getCurrentState();

        LookupTask task = new LookupTask(encounterSourceId, fhirResourceFiler, csvHelper, parserState);
        List<ThreadPoolError> errors = threadPool.submit(task);
        handleErrors(errors);
    }

    private static void handleErrors(List<ThreadPoolError> errors) throws Exception {
        if (errors == null || errors.isEmpty()) {
            return;
        }

        //if we've had multiple errors, just throw the first one, since the first exception is always most relevant
        ThreadPoolError first = errors.get(0);
        LookupTask callable = (LookupTask)first.getCallable();
        Throwable exception = first.getException();
        CsvCurrentState parserState = callable.getParserState();
        throw new TransformException(parserState.toString(), exception);
    }

    static class LookupTask implements Callable {

        private String encounterSourceId;
        private FhirResourceFiler fhirResourceFiler;
        private EmisCsvHelper csvHelper;
        private CsvCurrentState parserState;

        public LookupTask(String encounterSourceId,
                          FhirResourceFiler fhirResourceFiler,
                          EmisCsvHelper csvHelper,
                          CsvCurrentState parserState) {

            this.encounterSourceId = encounterSourceId;
            this.fhirResourceFiler = fhirResourceFiler;
            this.csvHelper = csvHelper;
            this.parserState = parserState;
        }

        @Override
        public Object call() throws Exception {
            try {
                //carry over linked items from any previous instance of this problem
                List<Reference> previousReferences = csvHelper.findPreviousLinkedReferences(fhirResourceFiler, encounterSourceId, ResourceType.Encounter);
                if (previousReferences != null && !previousReferences.isEmpty()) {
                    csvHelper.cacheConsultationPreviousLinkedResources(encounterSourceId, previousReferences);
                }
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