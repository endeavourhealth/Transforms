package org.endeavourhealth.transform.vision.transforms;

import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedListBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.vision.VisionCsvHelper;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class EncounterPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(EncounterPreTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 VisionCsvHelper csvHelper) throws Exception {

        //unlike most of the other parsers, we don't handle record-level exceptions and continue, since a failure
        //to parse any record in this file it a critical error
        try {
            AbstractCsvParser parser = parsers.get(org.endeavourhealth.transform.vision.schema.Encounter.class);
            if (parser != null) {
                while (parser.nextRecord()) {

                    try {
                        transform((org.endeavourhealth.transform.vision.schema.Encounter) parser, fhirResourceFiler, csvHelper);

                    } catch (Exception ex) {
                        throw new TransformException(parser.getCurrentState().toString(), ex);
                    }
                }
            }

        } finally {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void transform(org.endeavourhealth.transform.vision.schema.Encounter parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 VisionCsvHelper csvHelper) throws Exception {

        CsvCell consultationID = parser.getConsultationID();
        CsvCell patientID = parser.getPatientID();
        String encounterSourceId = VisionCsvHelper.createUniqueId(patientID, consultationID);

        CsvCurrentState parserState = parser.getCurrentState();

        LookupTask task = new LookupTask(encounterSourceId, csvHelper, parserState);
        csvHelper.submitToThreadPool(task);
    }

    static class LookupTask extends AbstractCsvCallable {

        private String encounterSourceId;
        private VisionCsvHelper csvHelper;

        public LookupTask(String encounterSourceId,
                          VisionCsvHelper csvHelper,
                          CsvCurrentState parserState) {
            super(parserState);
            this.encounterSourceId = encounterSourceId;
            this.csvHelper = csvHelper;
        }

        @Override
        public Object call() throws Exception {
            try {
                //carry over linked items from any previous instance of this encounter
                Encounter previousVersion = (Encounter)csvHelper.retrieveResource(encounterSourceId, ResourceType.Encounter);
                if (previousVersion == null) {
                    //if this is the first time, then we'll have a null resource
                    return null;
                }

                EncounterBuilder encounterBuilder = new EncounterBuilder(previousVersion);
                ContainedListBuilder containedListBuilder = new ContainedListBuilder(encounterBuilder);

                List<Reference> previousReferencesDiscoveryIds = containedListBuilder.getReferences();

                //the references will be mapped to Discovery UUIDs, so we need to convert them back to local IDs
                List<Reference> previousReferencesLocalIds = IdHelper.convertEdsReferencesToLocallyUniqueReferences(csvHelper, previousReferencesDiscoveryIds);

                csvHelper.cacheConsultationPreviousLinkedResources(encounterSourceId, previousReferencesLocalIds);

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }
    }
}