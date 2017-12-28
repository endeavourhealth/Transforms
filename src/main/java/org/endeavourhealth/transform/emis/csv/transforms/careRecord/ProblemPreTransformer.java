package org.endeavourhealth.transform.emis.csv.transforms.careRecord;

import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.transform.common.CsvCurrentState;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.exceptions.TransformException;
import org.endeavourhealth.transform.emis.csv.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.schema.AbstractCsvParser;
import org.endeavourhealth.transform.emis.csv.schema.careRecord.Problem;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class ProblemPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ProblemPreTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper,
                                 int maxFilingThreads) throws Exception {

        //use a thread pool to perform multiple lookups in parallel
        ThreadPool threadPool = new ThreadPool(maxFilingThreads, 50000);

        //unlike most of the other parsers, we don't handle record-level exceptions and continue, since a failure
        //to parse any record in this file it a critical error
        try {
            AbstractCsvParser parser = parsers.get(Problem.class);
            while (parser.nextRecord()) {

                try {
                    processLine((Problem) parser, fhirResourceFiler, csvHelper, version, threadPool);
                } catch (Exception ex) {
                    throw new TransformException(parser.getCurrentState().toString(), ex);
                }
            }

        } finally {
            List<ThreadPoolError> errors = threadPool.waitAndStop();
            handleErrors(errors);
        }
    }

    private static void processLine(Problem parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       EmisCsvHelper csvHelper,
                                       String version,
                                       ThreadPool threadPool) throws Exception {

        String patientGuid = parser.getPatientGuid();
        String observationGuid = parser.getObservationGuid();

        //cache the observation GUIDs of problems, so
        //that we know what is a problem when we run the observation pre-transformer
        csvHelper.cacheProblemObservationGuid(patientGuid, observationGuid, null);

        //also cache the IDs of any child items from previous instances of this problem, but
        //use a thread pool so we can perform multiple lookups in parallel
        String problemSourceId = EmisCsvHelper.createUniqueId(patientGuid, observationGuid);

        CsvCurrentState parserState = parser.getCurrentState();

        LookupTask task = new LookupTask(problemSourceId, fhirResourceFiler, csvHelper, parserState);
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

        private String problemSourceId;
        private FhirResourceFiler fhirResourceFiler;
        private EmisCsvHelper csvHelper;
        private CsvCurrentState parserState;

        public LookupTask(String problemSourceId,
                          FhirResourceFiler fhirResourceFiler,
                          EmisCsvHelper csvHelper,
                          CsvCurrentState parserState) {

            this.problemSourceId = problemSourceId;
            this.fhirResourceFiler = fhirResourceFiler;
            this.csvHelper = csvHelper;
            this.parserState = parserState;
        }

        @Override
        public Object call() throws Exception {
            try {
                //carry over linked items from any previous instance of this problem
                List<Reference> previousReferences = csvHelper.findPreviousLinkedReferences(fhirResourceFiler, problemSourceId, ResourceType.Condition);
                if (previousReferences != null && !previousReferences.isEmpty()) {
                    csvHelper.cacheProblemPreviousLinkedResources(problemSourceId, previousReferences);
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
