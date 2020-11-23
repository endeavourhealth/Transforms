package org.endeavourhealth.transform.vision.transforms;

import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.ConditionBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedListBuilder;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.vision.VisionCsvHelper;
import org.endeavourhealth.transform.vision.schema.Journal;
import org.hl7.fhir.instance.model.Condition;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class JournalProblemPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(JournalProblemPreTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 VisionCsvHelper csvHelper) throws Exception {

        //use a thread pool to perform multiple lookups in parallel
        int threadPoolSize = ConnectionManager.getPublisherTransformConnectionPoolMaxSize(fhirResourceFiler.getServiceId());
        ThreadPool threadPool = new ThreadPool(threadPoolSize, 1000, "VisionJournalProblem"); //lower from 50k to save memory

        AbstractCsvParser parser = parsers.get(Journal.class);

        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    processLine((Journal) parser, fhirResourceFiler, csvHelper, threadPool);
                } catch (Exception ex) {
                    List<ThreadPoolError> errors = threadPool.waitAndStop();
                    handleErrors(errors);
                }
            }
        }
    }

    private static void processLine(Journal parser,
                                    FhirResourceFiler fhirResourceFiler,
                                    VisionCsvHelper csvHelper,
                                    ThreadPool threadPool) throws Exception {

        if (parser.getAction().getString().equalsIgnoreCase("D")) {
            return;
        }

        //only concerned with Problems in this pre-transformer
        if (parser.getSubset().getString().equalsIgnoreCase("P")) {

            CsvCell patientID = parser.getPatientID();
            CsvCell observationID = parser.getObservationID();
            CsvCurrentState parserState = parser.getCurrentState();

            //cache the observation IDs of problems, so
            //that we know what is a problem when we run the observation pre-transformer
            csvHelper.cacheProblemObservationGuid(patientID, observationID);

            //also cache the IDs of any child items from previous instances of this problem, but
            //use a thread pool so we can perform multiple lookups in parallel
            JournalProblemPreTransformer.LookupTask task
                    = new JournalProblemPreTransformer.LookupTask(patientID, observationID, fhirResourceFiler, csvHelper, parserState);
            List<ThreadPoolError> errors = threadPool.submit(task);
            handleErrors(errors);
        }
    }

    private static void handleErrors(List<ThreadPoolError> errors) throws Exception {
        if (errors == null || errors.isEmpty()) {
            return;
        }

        //if we've had multiple errors, just throw the first one, since the first exception is always most relevant
        ThreadPoolError first = errors.get(0);
        JournalProblemPreTransformer.LookupTask callable = (JournalProblemPreTransformer.LookupTask)first.getCallable();
        Throwable exception = first.getException();
        CsvCurrentState parserState = callable.getParserState();
        throw new TransformException(parserState.toString(), exception);
    }

    static class LookupTask implements Callable {

        private CsvCell patientID;
        private CsvCell observationID;
        private FhirResourceFiler fhirResourceFiler;
        private VisionCsvHelper csvHelper;
        private CsvCurrentState parserState;

        public LookupTask(CsvCell patientID,
                          CsvCell observationID,
                          FhirResourceFiler fhirResourceFiler,
                          VisionCsvHelper csvHelper,
                          CsvCurrentState parserState) {

            this.patientID = patientID;
            this.observationID = observationID;
            this.fhirResourceFiler = fhirResourceFiler;
            this.csvHelper = csvHelper;
            this.parserState = parserState;
        }

        @Override
        public Object call() throws Exception {
            try {
                String locallyUniqueId = EmisCsvHelper.createUniqueId(patientID, observationID);

                //carry over linked items from any previous instance of this problem
                Condition previousVersion = (Condition)csvHelper.retrieveResource(locallyUniqueId, ResourceType.Condition);
                if (previousVersion == null) {
                    //if this is the first time, then we'll have a null resource
                    return null;
                }

                ConditionBuilder conditionBuilder = new ConditionBuilder(previousVersion);
                ContainedListBuilder containedListBuilder = new ContainedListBuilder(conditionBuilder);

                List<Reference> previousReferencesDiscoveryIds = containedListBuilder.getReferences();

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
