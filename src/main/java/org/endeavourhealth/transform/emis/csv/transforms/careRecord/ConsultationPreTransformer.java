package org.endeavourhealth.transform.emis.csv.transforms.careRecord;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedListBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.schema.careRecord.Consultation;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class ConsultationPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ConsultationPreTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        //unlike most of the other parsers, we don't handle record-level exceptions and continue, since a failure
        //to parse any record in this file it a critical error
        try {
            AbstractCsvParser parser = parsers.get(Consultation.class);
            while (parser.nextRecord()) {

                try {
                    processRecord((Consultation)parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    throw new TransformException(parser.getCurrentState().toString(), ex);
                }
            }

        } finally {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }

    public static void processRecord(Consultation parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      EmisCsvHelper csvHelper) throws Exception {

        CsvCell consultationGuid = parser.getConsultationGuid();
        CsvCell patientGuid = parser.getPatientGuid();

        String encounterSourceId = EmisCsvHelper.createUniqueId(patientGuid, consultationGuid);

        CsvCurrentState parserState = parser.getCurrentState();

        LookupTask task = new LookupTask(encounterSourceId, fhirResourceFiler, csvHelper, parserState);
        csvHelper.submitToThreadPool(task);
    }

    static class LookupTask extends AbstractCsvCallable {

        private String encounterSourceId;
        private FhirResourceFiler fhirResourceFiler;
        private EmisCsvHelper csvHelper;

        public LookupTask(String encounterSourceId,
                          FhirResourceFiler fhirResourceFiler,
                          EmisCsvHelper csvHelper,
                          CsvCurrentState parserState) {
            super(parserState);

            this.encounterSourceId = encounterSourceId;
            this.fhirResourceFiler = fhirResourceFiler;
            this.csvHelper = csvHelper;
        }

        @Override
        public Object call() throws Exception {
            try {
                //carry over linked items from any previous instance of this problem
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