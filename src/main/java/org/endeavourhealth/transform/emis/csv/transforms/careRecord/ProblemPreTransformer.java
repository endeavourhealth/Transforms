package org.endeavourhealth.transform.emis.csv.transforms.careRecord;

import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.ConditionBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedListBuilder;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.schema.careRecord.Problem;
import org.hl7.fhir.instance.model.Condition;
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
                                 EmisCsvHelper csvHelper) throws Exception {

        //use a thread pool to perform multiple lookups in parallel
        int threadPoolSize = ConnectionManager.getPublisherTransformConnectionPoolMaxSize(fhirResourceFiler.getServiceId());
        ThreadPool threadPool = new ThreadPool(threadPoolSize, 50000);

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

        CsvCell patientGuid = parser.getPatientGuid();
        CsvCell observationGuid = parser.getObservationGuid();
        CsvCurrentState parserState = parser.getCurrentState();

        //cache the observation GUIDs of problems, so
        //that we know what is a problem when we run the observation pre-transformer
        csvHelper.cacheProblemObservationGuid(patientGuid, observationGuid, null);

        //also cache the IDs of any child items from previous instances of this problem, but
        //use a thread pool so we can perform multiple lookups in parallel
        LookupTask task = new LookupTask(patientGuid, observationGuid, fhirResourceFiler, csvHelper, parserState);
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

        private CsvCell patientGuid;
        private CsvCell observationGuid;
        private FhirResourceFiler fhirResourceFiler;
        private EmisCsvHelper csvHelper;
        private CsvCurrentState parserState;

        public LookupTask(CsvCell patientGuid,
                          CsvCell observationGuid,
                          FhirResourceFiler fhirResourceFiler,
                          EmisCsvHelper csvHelper,
                          CsvCurrentState parserState) {

            this.patientGuid = patientGuid;
            this.observationGuid = observationGuid;
            this.fhirResourceFiler = fhirResourceFiler;
            this.csvHelper = csvHelper;
            this.parserState = parserState;
        }

        @Override
        public Object call() throws Exception {
            try {
                String locallyUniqueId = EmisCsvHelper.createUniqueId(patientGuid, observationGuid);

                //carry over linked items from any previous instance of this problem
                Condition previousVersion = (Condition)csvHelper.retrieveResource(locallyUniqueId, ResourceType.Condition, fhirResourceFiler);
                if (previousVersion == null) {
                    //if this is the first time, then we'll have a null resource
                    return null;
                }

                ConditionBuilder conditionBuilder = new ConditionBuilder(previousVersion);
                ContainedListBuilder containedListBuilder = new ContainedListBuilder(conditionBuilder);

                List<Reference> previousReferencesDiscoveryIds = containedListBuilder.getContainedListItems();

                //the references will be mapped to Discovery UUIDs, so we need to convert them back to local IDs
                List<Reference> previousReferencesLocalIds = IdHelper.convertEdsReferencesToLocallyUniqueReferences(csvHelper, previousReferencesDiscoveryIds);

                csvHelper.cacheProblemPreviousLinkedResources(locallyUniqueId, previousReferencesLocalIds);

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
