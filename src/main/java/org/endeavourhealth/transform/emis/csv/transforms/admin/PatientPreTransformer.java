package org.endeavourhealth.transform.emis.csv.transforms.admin;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedListBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.schema.admin.Patient;
import org.hl7.fhir.instance.model.EpisodeOfCare;
import org.hl7.fhir.instance.model.List_;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class PatientPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PatientPreTransformer.class);

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {


        //unlike most of the other parsers, we don't handle record-level exceptions and continue, since a failure
        //to parse any record in this file it a critical error
        try {
            AbstractCsvParser parser = parsers.get(Patient.class);
            while (parser.nextRecord()) {

                try {
                    processLine((Patient) parser, fhirResourceFiler, csvHelper, version);
                } catch (Exception ex) {
                    throw new TransformException(parser.getCurrentState().toString(), ex);
                }
            }

        } finally {
            csvHelper.waitUntilThreadPoolIsEmpty();
        }
    }

    private static void processLine(Patient parser,
                                    FhirResourceFiler fhirResourceFiler,
                                    EmisCsvHelper csvHelper,
                                    String version) throws Exception {

        CsvCell patientGuidCell = parser.getPatientGuid();
        CsvCell startDateCell = parser.getDateOfRegistration();
        CsvCurrentState state = parser.getCurrentState();

        PreCreateEdsPatientIdTask task = new PreCreateEdsPatientIdTask(state, patientGuidCell, startDateCell, fhirResourceFiler, csvHelper);
        csvHelper.submitToThreadPool(task);
    }

    static class PreCreateEdsPatientIdTask extends AbstractCsvCallable {

        private CsvCell patientGuidCell;
        private CsvCell startDateCell;
        private FhirResourceFiler fhirResourceFiler;
        private EmisCsvHelper csvHelper;

        public PreCreateEdsPatientIdTask(CsvCurrentState state,
                                         CsvCell patientGuidCell,
                                         CsvCell startDateCell,
                                         FhirResourceFiler fhirResourceFiler,
                                         EmisCsvHelper csvHelper) {
            super(state);

            this.patientGuidCell = patientGuidCell;
            this.startDateCell = startDateCell;
            this.fhirResourceFiler = fhirResourceFiler;
            this.csvHelper = csvHelper;
        }

        @Override
        public Object call() throws Exception {
            try {
                //just make the call into the ID helper, which will create and cache or just cache, so no creation is needed
                //when we get to the point of creating and saving resources, making it a bit faster
                String sourcePatientId = EmisCsvHelper.createUniqueId(patientGuidCell, null);
                IdHelper.getOrCreateEdsResourceIdString(fhirResourceFiler.getServiceId(), ResourceType.Patient, sourcePatientId);

                //we also want to cache the registration statuses from any pre-existing episode of care,
                //because we receive that in a separate custom extract, and don't want to lose it
                String sourceEpisodeId = EmisCsvHelper.createUniqueId(patientGuidCell, startDateCell);
                EpisodeOfCare existingEpisode = (EpisodeOfCare)csvHelper.retrieveResource(sourceEpisodeId, ResourceType.EpisodeOfCare);
                if (existingEpisode != null) {
                    EpisodeOfCareBuilder episodeOfCareBuilder = new EpisodeOfCareBuilder(existingEpisode);
                    ContainedListBuilder containedListBuilder = new ContainedListBuilder(episodeOfCareBuilder);
                    List<List_.ListEntryComponent> items = containedListBuilder.getContainedListItems();
                    if (items != null) {
                        csvHelper.cacheExistingRegistrationStatuses(sourceEpisodeId, items);
                    }
                }

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }

    }
}
