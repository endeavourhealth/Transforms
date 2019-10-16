package org.endeavourhealth.transform.emis.custom.helpers;

import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.schema.RegistrationStatus;
import org.endeavourhealth.common.fhir.schema.RegistrationType;
import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.common.AbstractCsvCallable;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedListBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.custom.transforms.RegistrationStatusTransformer;
import org.hl7.fhir.instance.model.CodeableConcept;
import org.hl7.fhir.instance.model.EpisodeOfCare;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;

public class EmisCustomCsvHelper {
    private static final Logger LOG = LoggerFactory.getLogger(EmisCustomCsvHelper.class);

    private static ResourceDalI resourceDal = DalProvider.factoryResourceDal();

    private Map<String, List<RegStatusObj>> regStatusCache = new HashMap<>();
    private ThreadPool utilityThreadPool = null;
    private UUID serviceId;

    public EmisCustomCsvHelper(UUID serviceId) {
        this.serviceId = serviceId;
    }

    public void saveRegistrationStatues(FhirResourceFiler fhirResourceFiler) throws Exception {

        if (regStatusCache.isEmpty()) {
            return;
        }

        LOG.debug("Saving reg statuses for " + regStatusCache.size() + " patients");

        for (String patientKey: regStatusCache.keySet()) {
            List<RegStatusObj> list = regStatusCache.get(patientKey);

            saveRegistrationStatuesForPatient(patientKey, list, fhirResourceFiler);
        }
    }

    private void saveRegistrationStatuesForPatient(String patientKey, List<RegStatusObj> regStatusList, FhirResourceFiler fhirResourceFiler) throws Exception {

        //the patient GUID in the standard extract files is in upper case and
        //has curly braces around it, so we need to ensure this is the same
        String patientGuidStr = "{" + patientKey.toUpperCase() + "}";

        //find the patient UUID for this patient GUID
        UUID patientUuid = IdHelper.getEdsResourceId(fhirResourceFiler.getServiceId(), ResourceType.Patient, patientGuidStr);

        //we can have no patient UUID if we get a reg status file before a bulk
        if (patientUuid == null) {
            return;
        }

        //get all episodes for that patient
        List<ResourceWrapper> episodeWrappers = resourceDal.getResourcesByPatient(fhirResourceFiler.getServiceId(), patientUuid, ResourceType.EpisodeOfCare.toString());
        if (episodeWrappers.isEmpty()) {
            //if no episodes, because we've not processed the bulk yet, skip it
            return;
        }

        //sort the reg statuses into date order, but handling the case where some have the same date
        regStatusList.sort((o1, o2) -> {
            return o1.compareTo(o2);
        });

        //create episode builders for each one
        List<EpisodeOfCareBuilder> builders = new ArrayList<>();
        for (ResourceWrapper wrapper: episodeWrappers) {
            String json = wrapper.getResourceData();
            EpisodeOfCare episodeOfCare = (EpisodeOfCare)FhirSerializationHelper.deserializeResource(json);
            EpisodeOfCareBuilder builder = new EpisodeOfCareBuilder(episodeOfCare);
            builders.add(builder);
        }

        //We're sent the full reg status history, but the regular extract only contains current reg details, so we have missing history.
        //We can use the past reg status records to infer previous registrations
        createMissingEpisodes(regStatusList, builders);

        //sort the episode builders into start date order
        builders.sort((o1, o2) -> {
            //note this will cause an exception if an episode doesn't have a start date, but Emis episodes always do
            Date d1 = o1.getRegistrationStartDate();
            Date d2 = o2.getRegistrationStartDate();
            return d1.compareTo(d2);
        });

        //clear down any reg statuses on each episode
        Map<EpisodeOfCareBuilder, ContainedListBuilder> hmListBuilders = new HashMap<>();
        for (EpisodeOfCareBuilder episodeBuilder: builders) {

            ContainedListBuilder containedListBuilder = new ContainedListBuilder(episodeBuilder);

            //remove existing statuses, since each time we get this file, it's a complete replacement
            containedListBuilder.removeContainedList();
            hmListBuilders.put(episodeBuilder, containedListBuilder);
        }

        //go through the reg statuses and add them to an appropriate episode
        for (RegStatusObj regStatus: regStatusList) {

            EpisodeOfCareBuilder builder = findEpisodeBuilderForRegStatus(regStatus, builders);
            if (builder == null) {
                continue;
            }

            ContainedListBuilder containedListBuilder = hmListBuilders.get(builder);

            CsvCell regStatusCell = regStatus.getRegStatusCell();
            RegistrationStatus registrationStatus = regStatus.convertRegistrationStatus();
            CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(registrationStatus);
            containedListBuilder.addCodeableConcept(codeableConcept, regStatusCell);

            CsvCell dateCell = regStatus.getDateTimeCell();
            containedListBuilder.addDateToLastItem(dateCell.getDateTime(), dateCell);

            //some old episodes may not have the reg type set, due to a past bug, so fix that if we spot it
            if (!builder.hasRegistrationType()) {
                CsvCell registrationTypeIdCell = regStatus.getRegTypeCell();
                RegistrationType registrationType = regStatus.convertRegistrationType();
                builder.setRegistrationType(registrationType, registrationTypeIdCell);
            }
        }

        //save the builders
        for (EpisodeOfCareBuilder builder: builders) {

            //detect if the builder is ID mapped or not, since we might be saving both old and new ones
            fhirResourceFiler.savePatientResource(null, !builder.isIdMapped(), builder);
        }
    }


    /**
     * old reg statuses can give us enough information to generate past episodes that ended before the feed was started
     */
    private void createMissingEpisodes(List<RegStatusObj> regStatusList, List<EpisodeOfCareBuilder> builders) throws Exception {

        //find the earliest reg date from the existing episodes (note the builders aren't sorted yet)
        Date earliestRegDate = null;
        for (EpisodeOfCareBuilder builder: builders) {
            Date d = builder.getRegistrationStartDate();
            if (earliestRegDate == null
                    || d.before(earliestRegDate)) {
                earliestRegDate = d;
            }
        }
        if (earliestRegDate == null) {
            throw new TransformException("Failed to find earliest reg date");
        }

        //find any statuses from before our earliest known date
        List<RegStatusObj> statusesBeforeFirstEpisode = new ArrayList<>();
        for (RegStatusObj regStatus: regStatusList) {
            Date d = regStatus.getDateTimeCell().getDate();
            if (d.before(earliestRegDate)) {
                statusesBeforeFirstEpisode.add(regStatus);
            }
        }

        CsvCell lastStartCell = null;

        for (RegStatusObj statusObj: statusesBeforeFirstEpisode) {
            RegistrationStatus status = statusObj.convertRegistrationStatus();

            if (RegistrationStatusTransformer.isDeductionRegistrationStatus(status)) {

                //if we've got a deduction status after a registration status, then that is a complete past-episode
                if (lastStartCell != null) {

                    EpisodeOfCareBuilder builder = createMissingEpisode(statusObj.getPatientGuidCell(), lastStartCell, statusObj.getDateTimeCell(), statusObj.getRegTypeCell(), statusObj.getOrganisationGuidCell());
                    builders.add(builder);

                    //reset these so we can spot the next start of a registration
                    lastStartCell = null;
                }

            } else {
                //see if we're the start of a new run
                if (lastStartCell == null) {
                    lastStartCell = statusObj.getDateTimeCell();
                }
            }
        }

    }

    private EpisodeOfCareBuilder createMissingEpisode(CsvCell patientGuidCell, CsvCell startDateCell, CsvCell endDateCell, CsvCell regTypeCell, CsvCell organisationGuidCell) throws Exception {

        //the date and GUIDs in the reg status file are formatted differently to the main extract file, so we need to
        //re-format all these to be consistent
        SimpleDateFormat sdf = new SimpleDateFormat(EmisCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD);

        Date startDate = startDateCell.getDate();
        String formattedStartDate = sdf.format(startDate);
        startDate = sdf.parse(formattedStartDate); //parse back into a Date object, so we lose any time element
        startDateCell = CsvCell.factoryWithNewValue(startDateCell, formattedStartDate);

        Date endDate = endDateCell.getDate();
        String formattedEndDate = sdf.format(endDate);
        endDate = sdf.parse(formattedEndDate); //parse back into a Date object, so we lose any time element
        endDateCell = CsvCell.factoryWithNewValue(endDateCell, formattedEndDate);

        String patientGuid = patientGuidCell.getString();
        String formattedPatientGuid = "{" + patientGuid.toUpperCase() + "}";
        patientGuidCell = CsvCell.factoryWithNewValue(patientGuidCell, formattedPatientGuid);

        String organisationGuid = organisationGuidCell.getString();
        String formattedOrganisationGuid = "{" + organisationGuid.toUpperCase() + "}";
        organisationGuidCell = CsvCell.factoryWithNewValue(organisationGuidCell, formattedOrganisationGuid);

        //now create the episode builder and populate with what we can
        EpisodeOfCareBuilder episodeBuilder = new EpisodeOfCareBuilder();
        EmisCsvHelper.setUniqueId(episodeBuilder, patientGuidCell, startDateCell);

        Reference patientReference = EmisCsvHelper.createPatientReference(patientGuidCell);
        episodeBuilder.setPatient(patientReference, patientGuidCell);

        //create a second reference, since it's not an immutable object
        Reference organisationReference = EmisCsvHelper.createOrganisationReference(organisationGuidCell);
        episodeBuilder.setManagingOrganisation(organisationReference, organisationGuidCell);

        RegistrationType registrationType = RegistrationStatusTransformer.convertRegistrationType(regTypeCell.getInt());
        episodeBuilder.setRegistrationType(registrationType, regTypeCell);

        episodeBuilder.setRegistrationStartDate(startDate, startDateCell);

        episodeBuilder.setRegistrationEndDate(endDate, endDateCell);

        return episodeBuilder;
    }

    /**
     * finds the most appropriate episode for the registration status record to go against
     */
    private EpisodeOfCareBuilder findEpisodeBuilderForRegStatus(RegStatusObj regStatus, List<EpisodeOfCareBuilder> builders) throws Exception {

        Date statusDate = regStatus.getDateTimeCell().getDate();
        RegistrationStatus status = regStatus.convertRegistrationStatus();

        if (RegistrationStatusTransformer.isDeductionRegistrationStatus(status)) {

            //if it's a deduction-type reg status...

            //if the status took place during an episode, use it
            List<EpisodeOfCareBuilder> episodesOnDate = findEpisodeBuildersActiveOnDate(statusDate, builders);
            if (!episodesOnDate.isEmpty()) {
                //if a patient was deducted and re-registered on the same date, we will return two episodes
                //for that date. In this case, use the first one, as that will be the ended one, and one most
                //relevant for this deducted reg status.
                return episodesOnDate.get(0);
            }

            //failing that, use the previously ended episode
            return findEpisodeBuilderEndedBeforeDate(statusDate, builders);

        } else {
            //if it's not a deduction-type status, then we simply want to use the episode that was active on the status date
            //if a patient was deducted and re-registered on the same date, we will return two episodes
            //for that date. In this case, use the first one, as that will be the ended one, and one most
            //relevant for this deducted reg status.
            List<EpisodeOfCareBuilder> episodesOnDate = findEpisodeBuildersActiveOnDate(statusDate, builders);
            if (!episodesOnDate.isEmpty()) {
                //if a patient was deducted and re-registered on the same date, we will return two episodes
                //for that date. In this case, use the last one, as that will be the active one, and one most
                //relevant for this non-deducted reg status.
                return episodesOnDate.get(episodesOnDate.size()-1);
            }

            //if there was no active registration on the date, then ignore the status
            return null;
        }
    }

    private EpisodeOfCareBuilder findEpisodeBuilderEndedBeforeDate(Date d, List<EpisodeOfCareBuilder> builders) {

        //the builders are in date order, so iterate back until we find the first end date before our date
        for (int i=builders.size()-1; i>=0; i--) {
            EpisodeOfCareBuilder builder = builders.get(i);

            Date endDate = builder.getRegistrationEndDate();
            if (endDate != null && endDate.before(d)) {
                return builder;
            }
        }

        return null;
    }

    private List<EpisodeOfCareBuilder> findEpisodeBuildersActiveOnDate(Date d, List<EpisodeOfCareBuilder> builders) {

        List<EpisodeOfCareBuilder> ret = new ArrayList<>();
        for (EpisodeOfCareBuilder builder: builders) {
            Date startDate = builder.getRegistrationStartDate();
            Date endDate = builder.getRegistrationEndDate();

            if (!startDate.after(d)
                    && (endDate == null || !endDate.before(d))) {
                ret.add(builder);
            }
        }

        return ret;
    }

    /*private static EpisodeOfCareBuilder findEpisodeBuilder(String patientKey, UUID serviceId) throws Exception {


        UUID globallyUniqueId = IdHelper.getEdsResourceId(serviceId, ResourceType.EpisodeOfCare, patientGuid);

        //if we've never heard of this patient before, skip it
        if (globallyUniqueId == null) {
            return null;
        }

        ResourceWrapper resourceHistory = resourceDal.getCurrentVersion(serviceId, ResourceType.EpisodeOfCare.toString(), globallyUniqueId);

        //if the resource has been deleted before, we'll have a null entry or one that says it's deleted
        if (resourceHistory == null
                || resourceHistory.isDeleted()) {
            return null;
        }

        String json = resourceHistory.getResourceData();
        EpisodeOfCare episodeOfCare = (EpisodeOfCare)FhirSerializationHelper.deserializeResource(json);
        return new EpisodeOfCareBuilder(episodeOfCare);
    }*/

    public void cacheRegStatus(CsvCell patientGuidCell, CsvCell regStatusCell, CsvCell dateTimeCell, CsvCell regTypeCell, CsvCell organisationGuidCell, Integer processingOrder) {

        RegStatusObj obj = new RegStatusObj(patientGuidCell, regStatusCell, dateTimeCell, regTypeCell, organisationGuidCell, processingOrder);

        String key = patientGuidCell.getString();
        List<RegStatusObj> list = regStatusCache.get(key);
        if (list == null) {
            list = new ArrayList<>();
            regStatusCache.put(key, list);
        }
        list.add(obj);
    }

    /*public void cacheRegStatus(CsvCell patientGuidCell, CsvCell regStatusCell, CsvCell regTypeCell, CsvCell dateCell, CsvCell processingOrderCell) {

        int processingOrder = processingOrderCell.getInt().intValue();
        RegStatusObj obj = new RegStatusObj(regStatusCell, dateCell, processingOrder);

        String key = patientGuidCell.getString();
        List<RegStatusObj> list = regStatusCache.get(key);
        if (list == null) {
            list = new ArrayList<>();
            regStatusCache.put(key, list);
        }
        list.add(obj);

        //and cache the reg type, which is the same for each reg status row
        regTypeCache.put(key, regTypeCell);
    }*/

    public void submitToThreadPool(Callable callable) throws Exception {
        if (this.utilityThreadPool == null) {
            int threadPoolSize = ConnectionManager.getPublisherTransformConnectionPoolMaxSize(serviceId);
            this.utilityThreadPool = new ThreadPool(threadPoolSize, 50000, "EmisCustomCsvHelper");
        }

        List<ThreadPoolError> errors = utilityThreadPool.submit(callable);
        AbstractCsvCallable.handleErrors(errors);
    }

    public void waitUntilThreadPoolIsEmpty() throws Exception {
        if (this.utilityThreadPool != null) {
            List<ThreadPoolError> errors = utilityThreadPool.waitUntilEmpty();
            AbstractCsvCallable.handleErrors(errors);
        }
    }

    public void stopThreadPool() throws Exception {
        if (this.utilityThreadPool != null) {
            List<ThreadPoolError> errors = utilityThreadPool.waitAndStop();
            AbstractCsvCallable.handleErrors(errors);
        }
    }

    class RegStatusObj {
        private CsvCell patientGuidCell;
        private CsvCell regStatusCell;
        private CsvCell dateTimeCell;
        private CsvCell regTypeCell;
        private CsvCell organisationGuidCell;
        private Integer processingOrder;

        public RegStatusObj(CsvCell patientGuidCell, CsvCell regStatusCell, CsvCell dateTimeCell, CsvCell regTypeCell, CsvCell organisationGuidCell, Integer processingOrder) {
            this.patientGuidCell = patientGuidCell;
            this.regStatusCell = regStatusCell;
            this.dateTimeCell = dateTimeCell;
            this.regTypeCell = regTypeCell;
            this.organisationGuidCell = organisationGuidCell;
            this.processingOrder = processingOrder;
        }

        public CsvCell getPatientGuidCell() {
            return patientGuidCell;
        }

        public CsvCell getRegStatusCell() {
            return regStatusCell;
        }

        public CsvCell getDateTimeCell() {
            return dateTimeCell;
        }

        public CsvCell getRegTypeCell() {
            return regTypeCell;
        }

        public CsvCell getOrganisationGuidCell() {
            return organisationGuidCell;
        }

        public Integer getProcessingOrder() {
            return processingOrder;
        }

        public RegistrationType convertRegistrationType() throws Exception {
            return RegistrationStatusTransformer.convertRegistrationType(regTypeCell.getInt());
        }

        public RegistrationStatus convertRegistrationStatus() throws Exception {
            return RegistrationStatusTransformer.convertRegistrationStatus(regStatusCell.getInt());
        }

        /*public int getProcessingOrder() {
            return processingOrder;
        }*/

        public int compareTo(RegStatusObj other) {

            //after going round the houses for over a year, we've agreeds that the records should be sorted by DATE TIME
            //and then only by PROCESSING ORDER if the date times are the same

            //sort by datetime
            try {
                Date d1 = getDateTimeCell().getDateTime();
                Date d2 = other.getDateTimeCell().getDateTime();

                int comp = d1.compareTo(d2);
                if (comp != 0) {
                    return comp;
                }
            } catch (Exception ex) {
                //need to handle potential exception from date format errors
                throw new RuntimeException("Failed to compare reg status objects", ex);
            }

            //sort by processing order
            if (processingOrder == null) {
                throw new RuntimeException("No processing order column in Registration Status file");
            }
            int comp = processingOrder.compareTo(other.getProcessingOrder());
            if (comp != 0) {
                return comp;
            }

            return 0;
        }
    }


}

