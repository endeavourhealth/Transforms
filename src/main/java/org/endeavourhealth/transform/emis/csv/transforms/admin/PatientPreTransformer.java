package org.endeavourhealth.transform.emis.csv.transforms.admin;

import org.endeavourhealth.common.fhir.schema.RegistrationType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
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

import java.util.*;

public class PatientPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PatientPreTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {


        //unlike most of the other parsers, we don't handle record-level exceptions and continue, since a failure
        //to parse any record in this file it a critical error
        try {
            Patient parser = (Patient)parsers.get(Patient.class);
            while (parser != null && parser.nextRecord()) {

                try {
                    if (csvHelper.shouldProcessRecord(parser)) {
                        processLine(parser, fhirResourceFiler, csvHelper);
                    }

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
                                    EmisCsvHelper csvHelper) throws Exception {

        CsvCell patientGuidCell = parser.getPatientGuid();
        CsvCell startDateCell = parser.getDateOfRegistration();
        CsvCell endDateCell = parser.getDateOfDeactivation();
        CsvCell regTypeCell = parser.getPatientTypeDescription();
        CsvCell dummyTypeCell = parser.getDummyType();
        CsvCell deletedCell = parser.getDeleted();
        CsvCurrentState state = parser.getCurrentState();

        PreCreateEdsPatientIdTask task = new PreCreateEdsPatientIdTask(state, patientGuidCell, startDateCell, endDateCell, regTypeCell, dummyTypeCell, deletedCell, fhirResourceFiler, csvHelper);
        csvHelper.submitToThreadPool(task);
    }

    static class PreCreateEdsPatientIdTask extends AbstractCsvCallable {

        private CsvCell patientGuidCell;
        private CsvCell startDateCell;
        private CsvCell endDateCell;
        private CsvCell regTypeCell;
        private CsvCell dummyTypeCell;
        private CsvCell deletedCell;
        private FhirResourceFiler fhirResourceFiler;
        private EmisCsvHelper csvHelper;

        public PreCreateEdsPatientIdTask(CsvCurrentState state,
                                         CsvCell patientGuidCell,
                                         CsvCell startDateCell,
                                         CsvCell endDateCell,
                                         CsvCell regTypeCell,
                                         CsvCell dummyTypeCell,
                                         CsvCell deletedCell,
                                         FhirResourceFiler fhirResourceFiler,
                                         EmisCsvHelper csvHelper) {
            super(state);

            this.patientGuidCell = patientGuidCell;
            this.startDateCell = startDateCell;
            this.endDateCell = endDateCell;
            this.regTypeCell = regTypeCell;
            this.dummyTypeCell = dummyTypeCell;
            this.deletedCell = deletedCell;
            this.fhirResourceFiler = fhirResourceFiler;
            this.csvHelper = csvHelper;
        }

        @Override
        public Object call() throws Exception {
            try {
                //just make the call into the ID helper, which will create and cache or just cache, so no creation is needed
                //when we get to the point of creating and saving resources, making it a bit faster
                String sourcePatientId = EmisCsvHelper.createUniqueId(patientGuidCell, null);
                UUID patientUuid = IdHelper.getOrCreateEdsResourceId(fhirResourceFiler.getServiceId(), ResourceType.Patient, sourcePatientId);

                if (!deletedCell.getBoolean()) {

                    //find a UUID for an existing EpisodeOfCare we should write to
                    String sourceEpisodeId = EmisCsvHelper.createUniqueId(patientGuidCell, startDateCell);
                    UUID episodeUuid = findEpisodeUuid(sourceEpisodeId, patientUuid);

                    //if we've matched to an existing episode, then hit the DB to cache any reg status data
                    if (episodeUuid != null) {
                        cacheEpisodeRegStatuses(sourceEpisodeId, episodeUuid);
                    }
                }

            } catch (Throwable t) {
                LOG.error("", t);
                throw t;
            }

            return null;
        }

        /**
         * we use the registration start date as part of the unique ID to allow multiple EpisodeOfCare resources
         * for one patient GUID. But we know the registration start date can be amended. So this checks to
         * see if a start date should be a new episode or belongs to an existing one
         */
        private UUID findEpisodeUuid(String sourceEpisodeId, UUID patientUuid) throws Exception {

            //see if we've just got an existing mapping using the start date we've just received
            UUID episodeUuid = IdHelper.getEdsResourceId(csvHelper.getServiceId(), ResourceType.EpisodeOfCare, sourceEpisodeId);
            if (episodeUuid != null) {
                return episodeUuid;
            }

            //if we've not generated a UUID for this source ID before then this is either a new episode or
            //an amended registration date to an existing episode. So we need to see if there's an existing episode
            //that we can match to, despite the difference in start date
            List<EpisodeOfCareBuilder> allEpisodes = new ArrayList<>();
            List<EpisodeOfCareBuilder> sameRegTypeEpisodes = new ArrayList<>();

            RegistrationType newRegType = PatientTransformer.convertRegistrationType(regTypeCell.getString(), dummyTypeCell.getBoolean());

            ResourceDalI resourceDal = DalProvider.factoryResourceDal();
            List<ResourceWrapper> episodeWrappers = resourceDal.getResourcesByPatient(csvHelper.getServiceId(), patientUuid, ResourceType.EpisodeOfCare.toString());

            for (ResourceWrapper episodeWrapper : episodeWrappers) {
                EpisodeOfCare episode = (EpisodeOfCare) episodeWrapper.getResource();
                EpisodeOfCareBuilder episodeBuilder = new EpisodeOfCareBuilder(episode);

                allEpisodes.add(episodeBuilder);

                //if this reg types differ, then don't allow it to be the same episode - we've seen patients change
                //reg type (e.g. Temporary to Regular) so let a new episode be created for the new registration type
                RegistrationType episodeRegType = episodeBuilder.getRegistrationType();
                if (newRegType == episodeRegType) {
                    sameRegTypeEpisodes.add(episodeBuilder);
                }
            }

            //sort the episodes so they're in date order
            allEpisodes.sort((o1, o2) -> {
                Date d1 = o1.getRegistrationStartDate();
                Date d2 = o2.getRegistrationStartDate();
                return d1.compareTo(d2);
            });

            sameRegTypeEpisodes.sort((o1, o2) -> {
                Date d1 = o1.getRegistrationStartDate();
                Date d2 = o2.getRegistrationStartDate();
                return d1.compareTo(d2);
            });

            //go through the episodes and see if we can match against one of them (only counting ones with the same reg type)
            Date newEndDate = endDateCell.getDate();
            episodeUuid = findBestEpisodeMatch(sameRegTypeEpisodes, newEndDate);

            //if we've found an episode that we should update, then set up a new mapping from source ID to the existing UUID
            //if no UUID is found it'll just generate a new one when it saves the EpisodeOfCare resource later
            if (episodeUuid != null) {
                IdHelper.getOrCreateEdsResourceId(csvHelper.getServiceId(), ResourceType.EpisodeOfCare, sourceEpisodeId, episodeUuid);
            }

            //make sure all other episodes are ended (except the one to be transformed). We are only ever in this
            //function when processing brand NEW data (never when re-processing) so just end everything except
            //the episode UUID we found.
            for (EpisodeOfCareBuilder episodeBuilder: allEpisodes) {
                Date episodeEndDate = episodeBuilder.getRegistrationEndDate();

                //match on active ones that AREN'T the episode UUID we've matched on
                if (episodeEndDate == null //active
                        && (episodeUuid == null || !episodeBuilder.getResourceIdAsUuid().equals(episodeUuid))) {

                    //end using the start of the current one
                    episodeBuilder.setRegistrationEndDate(startDateCell.getDate(), startDateCell);
                    fhirResourceFiler.savePatientResource(this.getParserState(), false, episodeBuilder);
                }
            }

            return episodeUuid;
        }

        private UUID findBestEpisodeMatch(List<EpisodeOfCareBuilder> episodes, Date newEndDate) {

            //always attempt to match to the earliest active episode if there is one
            //(if there are multiple active ones, then they'll be sorted out outside this fn)
            for (EpisodeOfCareBuilder episodeBuilder: episodes) {
                Date episodeEndDate = episodeBuilder.getRegistrationEndDate();
                if (episodeEndDate == null) {
                    return episodeBuilder.getResourceIdAsUuid();
                }
            }

            //if we don't have an active episode, then it may be possible we're changing the start date
            //of an already-ended episode (which does happen, rarely) so see if it's possible to match on end date
            if (newEndDate != null) {

                for (EpisodeOfCareBuilder episodeBuilder: episodes) {
                    Date episodeEndDate = episodeBuilder.getRegistrationEndDate();
                    if (episodeEndDate != null && newEndDate.equals(episodeEndDate)) {
                        return episodeBuilder.getResourceIdAsUuid();
                    }
                }
            }

            //if we make it here, we failed to make a match, so return null to allow a new episode to be created
            return null;
        }

        /**
         * we need to cache the reg status list
         * from the existing EpisodeOfCare, so the PatientTransformer can use it to generate the new version
         */
        private void cacheEpisodeRegStatuses(String sourceEpisodeId, UUID episodeUuid) throws Exception {

            ResourceDalI resourceDal = DalProvider.factoryResourceDal();
            EpisodeOfCare existingEpisode = (EpisodeOfCare)resourceDal.getCurrentVersionAsResource(csvHelper.getServiceId(), ResourceType.EpisodeOfCare, episodeUuid.toString());
            if (existingEpisode != null) {
                EpisodeOfCareBuilder episodeOfCareBuilder = new EpisodeOfCareBuilder(existingEpisode);
                ContainedListBuilder containedListBuilder = new ContainedListBuilder(episodeOfCareBuilder);
                List<List_.ListEntryComponent> items = containedListBuilder.getContainedListItems();
                if (items != null) {
                    csvHelper.cacheExistingRegistrationStatuses(sourceEpisodeId, items);
                }
            }
        }

        private static boolean equalsOrBothNull(Date d1, Date d2) {




            if (d1 == null && d2 == null) {
                return true;
            }

            if (d1 != null
                    && d2 != null
                    && d1.equals(d2)) {
                return true;
            }

            return false;
        }
    }
}
