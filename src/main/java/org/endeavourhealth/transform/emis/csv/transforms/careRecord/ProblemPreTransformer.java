package org.endeavourhealth.transform.emis.csv.transforms.careRecord;

import org.endeavourhealth.common.fhir.ReferenceHelper;
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

public class ProblemPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ProblemPreTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        //unlike most of the other parsers, we don't handle record-level exceptions and continue, since a failure
        //to parse any record in this file it a critical error
        try {
            AbstractCsvParser parser = parsers.get(Problem.class);
            while (parser != null && parser.nextRecord()) {

                try {
                    processLine((Problem) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    throw new TransformException(parser.getCurrentState().toString(), ex);
                }
            }

        } finally {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }

    private static void processLine(Problem parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       EmisCsvHelper csvHelper) throws Exception {

        CsvCell patientGuid = parser.getPatientGuid();
        CsvCell observationGuid = parser.getObservationGuid();
        CsvCurrentState parserState = parser.getCurrentState();

        //cache the observation GUIDs of problems, so
        //that we know what is a problem when we run the observation pre-transformer
        csvHelper.cacheProblemObservationGuid(patientGuid, observationGuid, null);

        //also cache the IDs of any child items from previous instances of this problem, but
        //use a thread pool so we can perform multiple lookups in parallel
        LookupTask task = new LookupTask(patientGuid, observationGuid, fhirResourceFiler, csvHelper, parserState);
        csvHelper.submitToThreadPool(task);
    }

    static class LookupTask extends AbstractCsvCallable {

        private CsvCell patientGuid;
        private CsvCell observationGuid;
        private FhirResourceFiler fhirResourceFiler;
        private EmisCsvHelper csvHelper;

        public LookupTask(CsvCell patientGuid,
                          CsvCell observationGuid,
                          FhirResourceFiler fhirResourceFiler,
                          EmisCsvHelper csvHelper,
                          CsvCurrentState parserState) {
            super(parserState);

            this.patientGuid = patientGuid;
            this.observationGuid = observationGuid;
            this.fhirResourceFiler = fhirResourceFiler;
            this.csvHelper = csvHelper;
        }

        @Override
        public Object call() throws Exception {
            try {
                String locallyUniqueId = EmisCsvHelper.createUniqueId(patientGuid, observationGuid);

                //carry over linked items from any previous instance of this problem
                Condition previousVersion = (Condition)csvHelper.retrieveResource(locallyUniqueId, ResourceType.Condition);
                if (previousVersion == null) {
                    //if this is the first time, then we'll have a null resource
                    return null;
                }

                ConditionBuilder conditionBuilder = new ConditionBuilder(previousVersion);
                ContainedListBuilder containedListBuilder = new ContainedListBuilder(conditionBuilder);

                List<Reference> previousReferencesDiscoveryIds = containedListBuilder.getReferences();

                //note: a previous bug has meant we've ended up with duplicate references in the contained list,
                //because Reference doesn't implement equals or hashCode. The below function fails if the same reference
                //is passed in twice, so we need to remove any duplicates here.
                ReferenceHelper.removeDuplicates(previousReferencesDiscoveryIds);

                //the references will be mapped to Discovery UUIDs, so we need to convert them back to local IDs
                List<Reference> previousReferencesLocalIds = IdHelper.convertEdsReferencesToLocallyUniqueReferences(csvHelper, previousReferencesDiscoveryIds);

                csvHelper.cacheProblemPreviousLinkedResources(locallyUniqueId, previousReferencesLocalIds);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }
}
