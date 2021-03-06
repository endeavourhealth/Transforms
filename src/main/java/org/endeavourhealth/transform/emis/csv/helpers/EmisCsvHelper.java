package org.endeavourhealth.transform.emis.csv.helpers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.HttpStatus;
import org.endeavourhealth.common.cache.ObjectMapperPool;
import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.common.fhir.schema.ProblemRelationshipType;
import org.endeavourhealth.common.fhir.schema.ProblemSignificance;
import org.endeavourhealth.common.security.keycloak.client.KeycloakClient;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.admin.ServiceDalI;
import org.endeavourhealth.core.database.dal.admin.models.Service;
import org.endeavourhealth.core.database.dal.audit.ExchangeDalI;
import org.endeavourhealth.core.database.dal.audit.models.Exchange;
import org.endeavourhealth.core.database.dal.audit.models.HeaderKeys;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.publisherCommon.EmisMissingCodeDalI;
import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisClinicalCode;
import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisCodeType;
import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisMissingCodes;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.referenceLists.ReferenceList;
import org.endeavourhealth.transform.common.referenceLists.ReferenceListNoCsvCells;
import org.endeavourhealth.transform.common.referenceLists.ReferenceListSingleCsvCells;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;
import org.endeavourhealth.transform.emis.csv.exceptions.EmisCodeNotFoundException;
import org.endeavourhealth.transform.emis.csv.schema.coding.ClinicalCodeType;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;



public class EmisCsvHelper implements HasServiceSystemAndExchangeIdI {
    private static final Logger LOG = LoggerFactory.getLogger(EmisCsvHelper.class);

    private static final String ID_DELIMITER = ":";

    private final UUID serviceId;
    private final UUID systemId;
    private final UUID exchangeId;
    private final String dataSharingAgreementGuid;
    private final Map<Class, AbstractCsvParser> parsers;
    private boolean processPatientData;
    private EmisPatientFiler patientFilter;

    //DB access
    private ResourceDalI resourceRepository = DalProvider.factoryResourceDal();

    //some resources are referred to by others, so we cache them here for when we need them
    private Boolean sharingAgreementDisabled = null;
    private ResourceCache<StringMemorySaver, ConditionBuilder> problemMap = new ResourceCache<>();
    private ResourceCache<StringMemorySaver, ReferralRequestBuilder> referralMap = new ResourceCache<>();
    private Map<StringMemorySaver, ReferenceList> observationChildMap = new HashMap<>(); //now keyed on just ObservationGUID and w/o PatientGUID
    private Map<StringMemorySaver, ReferenceList> newProblemChildren = new ConcurrentHashMap<>();
    private Map<StringMemorySaver, ReferenceList> consultationNewChildMap = new HashMap<>();
    private Map<StringMemorySaver, ReferenceList> consultationExistingChildMap = new ConcurrentHashMap<>(); //written to by many threads
    private Map<StringMemorySaver, IssueRecordIssueDate> drugRecordNewLastIssueDateMap = new ConcurrentHashMap<>();
    private Map<StringMemorySaver, IssueRecordIssueDate> drugRecordExistingLastIssueDateMap = new ConcurrentHashMap<>();
    private Map<StringMemorySaver, IssueRecordIssueDate> drugRecordNewFirstIssueDateMap = new ConcurrentHashMap<>();
    private Map<StringMemorySaver, List<BpComponent>> bpComponentMap = new HashMap<>();
    private Map<StringMemorySaver, SessionPractitioners> sessionPractitionerMap = new HashMap<>();
    //private Map<StringMemorySaver, List<CsvCell>> locationOrganisationMap = new HashMap<>();
    private Map<StringMemorySaver, CodeAndDate> ethnicityMap = new HashMap<>();
    private Map<StringMemorySaver, CodeAndDate> maritalStatusMap = new HashMap<>();
    private Map<StringMemorySaver, String> problemReadCodes = new HashMap<>();
    private Map<StringMemorySaver, ResourceType> parentObservationResourceTypes = new HashMap<>();
    private Map<StringMemorySaver, ReferenceList> problemPreviousLinkedResources = new ConcurrentHashMap<>(); //written to by many threads
    private Map<StringMemorySaver, List<List_.ListEntryComponent>> existingRegsitrationStatues = new ConcurrentHashMap<>();
    private ThreadPool utilityThreadPool = null;
    private Map<String, String> latestEpisodeStartDateCache = new HashMap<>();
    private EmisAdminHelper adminHelper = new EmisAdminHelper();

    //missing codes
    private Set<Long> foundMissingCodes = new HashSet<>();


    public EmisCsvHelper(UUID serviceId, UUID systemId, UUID exchangeId, String dataSharingAgreementGuid, Map<Class, AbstractCsvParser> parsers) {
        this.serviceId = serviceId;
        this.systemId = systemId;
        this.exchangeId = exchangeId;
        this.dataSharingAgreementGuid = dataSharingAgreementGuid;
        this.parsers = parsers;
    }


    @Override
    public UUID getServiceId() {
        return serviceId;
    }

    @Override
    public UUID getSystemId() {
        return systemId;
    }

    @Override
    public UUID getExchangeId() {
        return exchangeId;
    }


    public Map<Class, AbstractCsvParser> getParsers() {
        return parsers;
    }

    public String getDataSharingAgreementGuid() {
        return dataSharingAgreementGuid;
    }

    public boolean isProcessPatientData() {
        return processPatientData;
    }

    public void setProcessPatientData(boolean processPatientData) {
        this.processPatientData = processPatientData;
    }

    /**
     * to ensure globally unique IDs for all resources, a new ID is created
     * from the patientGuid and sourceGuid (e.g. observationGuid)
     */
    public static String createUniqueId(CsvCell patientGuid, CsvCell sourceGuid) {
        if (sourceGuid == null) {
            return patientGuid.getString();
        } else {
            return patientGuid.getString() + ID_DELIMITER + sourceGuid.getString();
        }
    }

    /*public static String createUniqueId(String patientGuid, String sourceGuid) {
        if (sourceGuid == null) {
            return patientGuid;
        } else {
            return patientGuid + ID_DELIMITER + sourceGuid;
        }
    }*/

    /*private static String getPatientGuidFromUniqueId(String uniqueId) {
        String[] toks = uniqueId.split(ID_DELIMITER);
        if (toks.length == 1
                || toks.length == 2) {
            return toks[0];
        } else {
            throw new IllegalArgumentException("Invalid unique ID string [" + uniqueId + "] - expect one or two tokens delimited with " + ID_DELIMITER);
        }
    }*/

    public static void setUniqueId(ResourceBuilderBase resourceBuilder, CsvCell patientGuid, CsvCell sourceGuid) {
        String resourceId = createUniqueId(patientGuid, sourceGuid);
        resourceBuilder.setId(resourceId, patientGuid, sourceGuid);
    }

    /*public static void setUniqueId(Resource resource, String patientGuid, String sourceGuid) {
        resource.setId(createUniqueId(patientGuid, sourceGuid));
    }*/



    /**
     * admin-type resources just use the EMIS CSV GUID as their reference
     */
    public Reference createPractitionerReference(CsvCell practitionerGuid) throws Exception {
        return ReferenceHelper.createReference(ResourceType.Practitioner, practitionerGuid.getString());
    }
    public static Reference createOrganisationReference(CsvCell organizationGuid) throws Exception {
        return ReferenceHelper.createReference(ResourceType.Organization, organizationGuid.getString());
    }
    public Reference createLocationReference(CsvCell locationGuid) throws Exception {
        return ReferenceHelper.createReference(ResourceType.Location, locationGuid.getString());
    }
    public Reference createScheduleReference(CsvCell scheduleGuid) throws Exception {
        return ReferenceHelper.createReference(ResourceType.Schedule, scheduleGuid.getString());
    }
    public Reference createSlotReference(CsvCell patientGuid, CsvCell slotGuid) throws Exception {
        String resourceId = createUniqueId(patientGuid, slotGuid);
        return ReferenceHelper.createReference(ResourceType.Slot, resourceId);
    }


    /*public Reference createLocationReference(String locationGuid) throws Exception {
        return ReferenceHelper.createReference(ResourceType.Location, locationGuid);
    }
    public Reference createOrganisationReference(String organizationGuid) throws Exception {
        return ReferenceHelper.createReference(ResourceType.Organization, organizationGuid);
    }
    public Reference createPractitionerReference(String practitionerGuid) throws Exception {
        return ReferenceHelper.createReference(ResourceType.Practitioner, practitionerGuid);
    }
    public Reference createScheduleReference(String scheduleGuid) throws Exception {
        return ReferenceHelper.createReference(ResourceType.Schedule, scheduleGuid);
    }
    public Reference createSlotReference(String slotGuid) throws Exception {
        return ReferenceHelper.createReference(ResourceType.Slot, slotGuid);
    }*/

    /**
     * patient-type resources must include the patient GUID are part of the unique ID in the reference
     * because the EMIS GUIDs for things like Obs are only unique within that patient record itself
     */
    public static Reference createPatientReference(CsvCell patientGuid) throws Exception {
        return ReferenceHelper.createReference(ResourceType.Patient, createUniqueId(patientGuid, null));
    }
    public Reference createAppointmentReference(CsvCell appointmentGuid, CsvCell patientGuid) throws Exception {
        if (appointmentGuid.isEmpty()) {
            throw new IllegalArgumentException("Missing appointmentGuid");
        }
        return ReferenceHelper.createReference(ResourceType.Appointment, createUniqueId(patientGuid, appointmentGuid));
    }
    public Reference createEncounterReference(CsvCell encounterGuid, CsvCell patientGuid) throws Exception {
        if (encounterGuid.isEmpty()) {
            throw new IllegalArgumentException("Missing encounterGuid");
        }
        return ReferenceHelper.createReference(ResourceType.Encounter, createUniqueId(patientGuid, encounterGuid));
    }
    public Reference createConditionReference(CsvCell problemGuid, CsvCell patientGuid) {
        if (problemGuid.isEmpty()) {
            throw new IllegalArgumentException("Missing problemGuid");
        }
        return ReferenceHelper.createReference(ResourceType.Condition, createUniqueId(patientGuid, problemGuid));
    }
    public Reference createMedicationStatementReference(CsvCell medicationStatementGuid, CsvCell patientGuid) throws Exception {
        if (medicationStatementGuid.isEmpty()) {
            throw new IllegalArgumentException("Missing medicationStatementGuid");
        }
        return ReferenceHelper.createReference(ResourceType.MedicationStatement, createUniqueId(patientGuid, medicationStatementGuid));
    }
    public Reference createProblemReference(CsvCell problemGuid, CsvCell patientGuid) throws Exception {
        if (problemGuid.isEmpty()) {
            throw new IllegalArgumentException("Missing problemGuid");
        }
        return ReferenceHelper.createReference(ResourceType.Condition, createUniqueId(patientGuid, problemGuid));
    }

    public Reference createEpisodeReference(CsvCell patientGuid) throws Exception{
        //we now use registration start date as part of the source ID for episodes of care, so we use the internal  ID map table to store that start date
        CsvCell regStartCell = getLatestEpisodeStartDate(patientGuid);
        if (regStartCell == null) {

            //if we fail to find a reg data for a patient it's either because we've messed up and processed files
            //out of order (bad) or we're re-playing data for a patient record that was subsequently deleted (OK).
            //Work out which is the reason before choosing the throw an exception
            boolean logError = true;

            String sourcePatientId = createUniqueId(patientGuid, null);
            UUID patientUuid = IdHelper.getEdsResourceId(serviceId, ResourceType.Patient, sourcePatientId);
            List<ResourceWrapper> patientWrappers = resourceRepository.getResourceHistory(serviceId, ResourceType.Patient.toString(), patientUuid);
            if (patientWrappers.isEmpty()) {
                LOG.debug("No FHIR Patient history found for UUID " + patientGuid);

            } else {
                ResourceWrapper mostRecent = patientWrappers.get(0); //most recent is first
                if (mostRecent.isDeleted()) {
                    LOG.debug("FHIR Patient is deleted");
                    logError = false;

                } else {
                    LOG.debug("FHIR Patient is not deleted");
                }
            }

            if (logError) {
                throw new Exception("Failed to find latest registration date for patient " + patientGuid);

            } else {
                return null;
            }
        }

        String episodeSourceId = createUniqueId(patientGuid, regStartCell);
        return ReferenceHelper.createReference(ResourceType.EpisodeOfCare, episodeSourceId);
    }

    public void cacheLatestEpisodeStartDate(CsvCell patientGuid, CsvCell startDateCell) throws Exception {
        String key = patientGuid.getString();
        String value = startDateCell.getString();

        latestEpisodeStartDateCache.put(key, value);
    }

    public CsvCell getLatestEpisodeStartDate(CsvCell patientGuid) throws Exception {
        String key = patientGuid.getString();
        String value = latestEpisodeStartDateCache.get(key);
        if (value == null) {

            //convert patient GUID to UUID
            String sourcePatientId = createUniqueId(patientGuid, null);
            UUID patientUuid = IdHelper.getEdsResourceId(serviceId, ResourceType.Patient, sourcePatientId);
            if (patientUuid == null) {
                return null;
            }

            List<EpisodeOfCareBuilder> episodeBuilders = new ArrayList<>();

            List<ResourceWrapper> episodeWrappers = resourceRepository.getResourcesByPatient(serviceId, patientUuid, ResourceType.EpisodeOfCare.toString());
            for (ResourceWrapper episodeWrapper: episodeWrappers) {
                EpisodeOfCare episode = (EpisodeOfCare)episodeWrapper.getResource();
                EpisodeOfCareBuilder builder = new EpisodeOfCareBuilder(episode);
                episodeBuilders.add(builder);
            }

            //sort by reg date
            episodeBuilders.sort((o1, o2) -> {
                Date d1 = o1.getRegistrationStartDate();
                Date d2 = o2.getRegistrationStartDate();
                return d1.compareTo(d2);
            });

            if (episodeBuilders.isEmpty()) {
                return null;
            }

            EpisodeOfCareBuilder latestEpisode = episodeBuilders.get(episodeBuilders.size()-1);
            Date latestDate = latestEpisode.getRegistrationStartDate();

            //need to convert back to a string in the same format as the raw file
            SimpleDateFormat sdf = new SimpleDateFormat(EmisCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD);
            value = sdf.format(latestDate);
            latestEpisodeStartDateCache.put(key, value);

            //if this episode was created by the Reg Status transform, then it won't have a mapping with the normally
            //formatted start date, so make sure one exists
            String sourceId = createUniqueId(patientGuid, CsvCell.factoryDummyWrapper(value));
            UUID episodeUuid = latestEpisode.getResourceIdAsUuid();
            IdHelper.getOrCreateEdsResourceId(serviceId, ResourceType.EpisodeOfCare, sourceId, episodeUuid);
        }

        return CsvCell.factoryDummyWrapper(value);
    }

    public void cacheReferral(CsvCell observationGuid, CsvCell patientGuid, ReferralRequestBuilder referralRequestBuilder) throws Exception {
        String uid = createUniqueId(patientGuid, observationGuid);
        StringMemorySaver key = new StringMemorySaver(uid);
        referralMap.addToCache(key, referralRequestBuilder);
    }

    public ReferralRequestBuilder findReferral(CsvCell observationGuid, CsvCell patientGuid) throws Exception {
        String uid = createUniqueId(patientGuid, observationGuid);
        StringMemorySaver key = new StringMemorySaver(uid);
        return referralMap.getAndRemoveFromCache(key);
    }

    public void cacheProblem(CsvCell observationGuid, CsvCell patientGuid, ConditionBuilder conditionBuilder) throws Exception {
        //cache the resource as a JSON string, as the FHIR conditions take about 4KB memory EACH
        String uid = createUniqueId(patientGuid, observationGuid);
        StringMemorySaver key = new StringMemorySaver(uid);
        problemMap.addToCache(key, conditionBuilder);
    }

    public boolean existsProblem(CsvCell observationGuid, CsvCell patientGuid) {
        String uid = createUniqueId(patientGuid, observationGuid);
        StringMemorySaver key = new StringMemorySaver(uid);
        return problemMap.contains(key);
    }

    public ConditionBuilder findProblem(CsvCell observationGuid, CsvCell patientGuid) throws Exception {
        String uid = createUniqueId(patientGuid, observationGuid);
        StringMemorySaver key = new StringMemorySaver(uid);
        return problemMap.getAndRemoveFromCache(key);
    }


    public ReferenceList getAndRemoveObservationParentRelationships(String parentObservationSourceId) {
        StringMemorySaver key = new StringMemorySaver(parentObservationSourceId);
        return observationChildMap.remove(key);
    }



    public void cacheObservationParentRelationship(CsvCell parentObservationGuid,
                                                   CsvCell patientGuid,
                                                   CsvCell observationGuid,
                                                   ResourceType resourceType) {

        String parentObservationUniqueId = createUniqueId(patientGuid, parentObservationGuid);
        StringMemorySaver key = new StringMemorySaver(parentObservationUniqueId);
        ReferenceList list = observationChildMap.get(key);
        if (list == null) {

            list = new ReferenceListSingleCsvCells(); //we know there will only be a single cell, so use this reference list class to save memory

            observationChildMap.put(key, list);
        }

        String childObservationUniqueId = createUniqueId(patientGuid, observationGuid);
        list.add(resourceType, childObservationUniqueId, observationGuid);
    }


    public Resource retrieveResource(String locallyUniqueId, ResourceType resourceType) throws Exception {

        UUID globallyUniqueId = IdHelper.getEdsResourceId(serviceId, resourceType, locallyUniqueId);

        //if we've never mapped the local ID to a EDS UI, then we've never heard of this resource before
        if (globallyUniqueId == null) {
            return null;
        }

        ResourceWrapper resourceHistory = resourceRepository.getCurrentVersion(serviceId, resourceType.toString(), globallyUniqueId);

        //if the resource has been deleted before, we'll have a null entry or one that says it's deleted
        if (resourceHistory == null
                || resourceHistory.isDeleted()) {
            return null;
        }

        String json = resourceHistory.getResourceData();
        try {
            return FhirSerializationHelper.deserializeResource(json);
        } catch (Throwable t) {
            throw new Exception("Error deserialising " + resourceType + " " + globallyUniqueId + " (raw ID " + locallyUniqueId + ")", t);
        }

    }

    /**
     * as the end of processing all CSV files, there may be some new observations that link
     * to past parent observations. These linkages are saved against the parent observation,
     * so we need to retrieve them off the main repository, amend them and save them
     */
    public void processRemainingObservationParentChildLinks(FhirResourceFiler fhirResourceFiler) throws Exception {
        List<StringMemorySaver> keys = new ArrayList<>(observationChildMap.keySet());
        for (StringMemorySaver key: keys) {
            String uid = key.toString();
            ReferenceList referenceList = observationChildMap.get(key);
            updateExistingObservationWithNewChildLinks(uid, referenceList, fhirResourceFiler);
        }
    }


    private void updateExistingObservationWithNewChildLinks(String locallyUniqueId,
                                                            ReferenceList childResourceRelationships,
                                                            FhirResourceFiler fhirResourceFiler) throws Exception {

        //the parent "observation" may have been saved as an observation OR a diagnostic report, so try both
        ResourceBuilderBase resourceBuilder;

        DiagnosticReport fhirDiagnosticReport = (DiagnosticReport)retrieveResource(locallyUniqueId, ResourceType.DiagnosticReport);
        if (fhirDiagnosticReport != null) {
            resourceBuilder = new DiagnosticReportBuilder(fhirDiagnosticReport);
        } else {
            Observation fhirObservation = (Observation)retrieveResource(locallyUniqueId, ResourceType.Observation);
            if (fhirObservation != null) {
                resourceBuilder = new ObservationBuilder(fhirObservation);
            } else {
                //if the resource can't be found (or isn't an Observation or DiagnosticReport), then we can't update it
                return;
            }
        }

        boolean changed = false;

        for (int i=0; i<childResourceRelationships.size(); i++) {
            Reference reference = childResourceRelationships.getReference(i);
            CsvCell[] sourceCells = childResourceRelationships.getSourceCells(i);

            //the resource has already been ID mapped, so we need to manually convert our local reference to a global one
            Reference globallyUniqueReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(reference, fhirResourceFiler);

            if (resourceBuilder instanceof DiagnosticReportBuilder) {
                DiagnosticReportBuilder diagnosticReportBuilder = (DiagnosticReportBuilder)resourceBuilder;
                if (diagnosticReportBuilder.addResult(globallyUniqueReference, sourceCells)) {
                    changed = true;
                }

            } else {
                ObservationBuilder observationBuilder = (ObservationBuilder)resourceBuilder;
                if (observationBuilder.addChildObservation(globallyUniqueReference, sourceCells)) {
                    changed = true;
                }
            }
        }

        if (changed) {
            //make sure to pass in the parameter to bypass ID mapping, since this resource has already been done
            fhirResourceFiler.savePatientResource(null, false, resourceBuilder);
        }
    }

    public ReferenceList getAndRemoveNewProblemChildren(CsvCell problemGuid, CsvCell patientGuid) {
        String uid = createUniqueId(patientGuid, problemGuid);
        StringMemorySaver key = new StringMemorySaver(uid);
        return newProblemChildren.remove(key);
    }

    public void cacheProblemRelationship(CsvCell problemObservationGuid,
                                         CsvCell patientGuid,
                                         CsvCell resourceGuid,
                                         ResourceType resourceType) {

        if (problemObservationGuid.isEmpty()) {
            return;
        }

        String problemLocalUniqueId = createUniqueId(patientGuid, problemObservationGuid);
        StringMemorySaver key = new StringMemorySaver(problemLocalUniqueId);
        ReferenceList referenceList = newProblemChildren.get(key);
        if (referenceList == null) {
            //we know there will only be a single cell, so use this reference list class to save memory
            referenceList = new ReferenceListSingleCsvCells();
            //referenceList = new ReferenceList();
            newProblemChildren.put(key, referenceList);
        }

        String resourceLocalUniqueId = createUniqueId(patientGuid, resourceGuid);
        Reference reference = ReferenceHelper.createReference(resourceType, resourceLocalUniqueId);
        referenceList.add(reference, problemObservationGuid);
    }

    /**
     * called at the end of the transform, to update pre-existing Problem resources with references to new
     * clinical resources that are in those problems
     */
    public void processRemainingProblemRelationships(FhirResourceFiler fhirResourceFiler) throws Exception {
        for (StringMemorySaver key: newProblemChildren.keySet()) {
            ReferenceList newLinkedItems = newProblemChildren.get(key);

            String locallyUniqueResourceProblemId = key.toString();
            Condition existingCondition = (Condition)retrieveResource(locallyUniqueResourceProblemId, ResourceType.Condition);
            if (existingCondition == null) {
                //if the problem has been deleted, just skip it
                return;
            }

            ConditionBuilder conditionBuilder = new ConditionBuilder(existingCondition);
            ContainedListBuilder containedListBuilder = new ContainedListBuilder(conditionBuilder);

            for (int i=0; i<newLinkedItems.size(); i++) {
                Reference reference = newLinkedItems.getReference(i);
                CsvCell[] sourceCells = newLinkedItems.getSourceCells(i);

                Reference globallyUniqueReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(reference, fhirResourceFiler);
                containedListBuilder.addReference(globallyUniqueReference, sourceCells);
            }

            fhirResourceFiler.savePatientResource(null, false, conditionBuilder);
        }
    }



    /*public List<Reference> findPreviousLinkedReferences(FhirResourceFiler fhirResourceFiler,
                                                       String locallyUniqueId,
                                                       ResourceType resourceType) throws Exception {

        DomainResource previousVersion = (DomainResource)retrieveResource(locallyUniqueId, resourceType, fhirResourceFiler);
        if (previousVersion == null) {
            //if this is the first time, then we'll have a null resource
            return null;
        }
        List<Reference> edsReferences = new ArrayList<>();

        if (previousVersion.hasContained()) {
            for (Resource contained: previousVersion.getContained()) {
                if (contained instanceof List_) {
                    List_ list = (List_)contained;

                    for (List_.ListEntryComponent entry: list.getEntry()) {
                        Reference previousReference = entry.getItem();
                        edsReferences.add(previousReference);
                    }
                }
            }
        }

        UUID serviceId = fhirResourceFiler.getServiceId();

        //the reference we have has already been mapped to an EDS ID, so we need to un-map it
        //back to the source ID, so the ID mapper can safely map it when we save the resource
        return IdHelper.convertEdsReferencesToLocallyUniqueReferences(serviceId, edsReferences);
    }*/

    public void cacheNewDrugRecordDate(CsvCell drugRecordGuid, CsvCell patientGuid, IssueRecordIssueDate newIssueDate) {
        String uniqueId = createUniqueId(patientGuid, drugRecordGuid);
        StringMemorySaver key = new StringMemorySaver(uniqueId);

        IssueRecordIssueDate previous = drugRecordNewFirstIssueDateMap.get(key);
        if (newIssueDate.beforeOrOtherIsNull(previous)) {
            drugRecordNewFirstIssueDateMap.put(key, newIssueDate);
        }

        previous = drugRecordNewLastIssueDateMap.get(key);
        if (newIssueDate.afterOrOtherIsNull(previous)) {
            drugRecordNewLastIssueDateMap.put(key, newIssueDate);
        }
    }

    public IssueRecordIssueDate getNewDrugRecordFirstIssueDate(CsvCell drugRecordId, CsvCell patientGuid) {
        String uniqueId = createUniqueId(patientGuid, drugRecordId);
        StringMemorySaver key = new StringMemorySaver(uniqueId);
        return drugRecordNewFirstIssueDateMap.remove(key);
    }

    public IssueRecordIssueDate getNewDrugRecordLastIssueDate(CsvCell drugRecordId, CsvCell patientGuid) {
        String uniqueId = createUniqueId(patientGuid, drugRecordId);
        StringMemorySaver key = new StringMemorySaver(uniqueId);
        return drugRecordNewLastIssueDateMap.remove(key);
    }

    public boolean hasNewDrugRecordLastIssueDate(CsvCell drugRecordId, CsvCell patientGuid) {
        String uniqueId = createUniqueId(patientGuid, drugRecordId);
        StringMemorySaver key = new StringMemorySaver(uniqueId);
        return drugRecordNewLastIssueDateMap.containsKey(key);
    }

    public void cacheExistingDrugRecordDate(CsvCell drugRecordGuid, CsvCell patientGuid, IssueRecordIssueDate newIssueDate) {
        String uniqueId = createUniqueId(patientGuid, drugRecordGuid);
        StringMemorySaver key = new StringMemorySaver(uniqueId);

        drugRecordExistingLastIssueDateMap.put(key, newIssueDate);
    }

    public IssueRecordIssueDate getExistingDrugRecordLastIssueDate(CsvCell drugRecordIdCell, CsvCell patientGuidCell) {
        String uniqueId = createUniqueId(patientGuidCell, drugRecordIdCell);
        StringMemorySaver key = new StringMemorySaver(uniqueId);
        return drugRecordExistingLastIssueDateMap.remove(key);
    }



    public void cacheBpComponent(CsvCell parentObservationGuid, CsvCell patientGuid, BpComponent bpComponent) {
        String uniqueId = createUniqueId(patientGuid, parentObservationGuid);
        StringMemorySaver key = new StringMemorySaver(uniqueId);

        List<BpComponent> list = bpComponentMap.get(key);
        if (list == null) {
            list = new ArrayList<>();
            bpComponentMap.put(key, list);
        }
        list.add(bpComponent);

        //Emis seem to have started sending BP readings diastolic first, so impose sorting to undo this since systolic is always the higher
        list.sort((o1, o2) -> {
            Double d1 = o1.getValue().getDouble();
            Double d2 = o2.getValue().getDouble();
            return d2.compareTo(d1);
        });
    }

    public List<BpComponent> findBpComponents(CsvCell observationGuid, CsvCell patientGuid) {
        String uniqueId = createUniqueId(patientGuid, observationGuid);
        StringMemorySaver key = new StringMemorySaver(uniqueId);
        return bpComponentMap.remove(key);
    }

    public void cacheSessionPractitionerMap(CsvCell sessionCell, CsvCell emisUserGuid, boolean isDeleted) {

        String sessionGuid = sessionCell.getString();
        StringMemorySaver key = new StringMemorySaver(sessionGuid);

        SessionPractitioners obj = sessionPractitionerMap.get(key);
        if (obj == null) {
            obj = new SessionPractitioners();
            sessionPractitionerMap.put(key, obj);
        }

        if (isDeleted) {
            obj.getEmisUserGuidsToDelete().add(emisUserGuid);
        } else {
            obj.getEmisUserGuidsToSave().add(emisUserGuid);
        }
    }

    public List<CsvCell> findSessionPractitionersToSave(CsvCell sessionCell) {

        String sessionGuid = sessionCell.getString();

        //unlike the other maps, we don't remove from this map, since we need to be able to look up
        //the staff for a session when creating Schedule resources and Appointment ones
        StringMemorySaver key = new StringMemorySaver(sessionGuid);
        SessionPractitioners obj = sessionPractitionerMap.get(key);
        if (obj == null) {
            return new ArrayList<>();

        } else {
            return obj.getEmisUserGuidsToSave();
        }
    }

    public List<CsvCell> findSessionPractitionersToDelete(CsvCell sessionCell) {

        String sessionGuid = sessionCell.getString();

        //unlike the other maps, we don't remove from this map, since we need to be able to look up
        //the staff for a session when creating Schedule resources and Appointment ones
        StringMemorySaver key = new StringMemorySaver(sessionGuid);
        SessionPractitioners obj = sessionPractitionerMap.get(key);
        if (obj == null) {
            return new ArrayList<>();

        } else {
            return obj.getEmisUserGuidsToDelete();
        }
    }

    public void clearCachedSessionPractitioners() {
        //set to null so any attempts to use it will cause a null pointer
        sessionPractitionerMap = null;
    }

    /*public void cacheOrganisationLocationMap(CsvCell locationCell, CsvCell orgCell, boolean mainLocation) {

        String locationGuid = locationCell.getString();
        StringMemorySaver key = new StringMemorySaver(locationGuid);

        List<CsvCell> orgGuids = locationOrganisationMap.get(key);
        if (orgGuids == null) {
            orgGuids = new ArrayList<>();
            locationOrganisationMap.put(key, orgGuids);
        }

        //if this location link is for the main location of an organisation, then insert that
        //org at the start of the list, so it's used as the managing organisation for the location
        if (mainLocation) {
            orgGuids.add(0, orgCell);
        } else {
            orgGuids.add(orgCell);
        }
    }

    public List<CsvCell> findOrganisationLocationMapping(CsvCell locationCell) {
        String locationGuid = locationCell.getString();
        StringMemorySaver key = new StringMemorySaver(locationGuid);
        return locationOrganisationMap.remove(key);
    }

    *//**
     * called at the end of the transform to handle any changes to location/organisation mappings
     * that weren't handled when we went through the Location file (i.e. changes in the OrganisationLocation file
     * with no corresponding changes in the Location file)
     *//*
    public void processRemainingOrganisationLocationMappings(FhirResourceFiler fhirResourceFiler) throws Exception {

        for (StringMemorySaver key: locationOrganisationMap.keySet()) {

            String locationGuid = key.toString();
            List<CsvCell> cells = locationOrganisationMap.get(key);

            Location fhirLocation = (Location)retrieveResource(locationGuid, ResourceType.Location);
            if (fhirLocation == null) {
                //if the location has been deleted, it doesn't matter, and the emis data integrity issues
                //mean we may have references to unknown locations
                continue;
            }

            LocationBuilder locationBuilder = new LocationBuilder(fhirLocation);

            CsvCell organisationCell = cells.get(0);
            String organisationGuid = organisationCell.getString();

            //the resource has already been through the ID mapping process, so we need to manually map the organisation GUID to discovery UUID
            String globallyUniqueId = IdHelper.getOrCreateEdsResourceIdString(fhirResourceFiler.getServiceId(),
                    ResourceType.Organization,
                    organisationGuid);

            Reference reference = ReferenceHelper.createReference(ResourceType.Organization, globallyUniqueId);
            locationBuilder.setManagingOrganisation(reference, organisationCell);

            fhirResourceFiler.saveAdminResource(null, false, locationBuilder);
        }
    }*/

    public void cacheEthnicity(CsvCell patientGuid, CodeAndDate codeAndDate) throws Exception {
        String localUniquePatientId = createUniqueId(patientGuid, null);
        StringMemorySaver key = new StringMemorySaver(localUniquePatientId);
        CodeAndDate existing = ethnicityMap.get(key);

        boolean addToMap = false;

        //if this new ethnicity code is newer (or there isn't already one cached) then add it
        if (codeAndDate.isAfterOrOtherIsNull(existing)) {
            addToMap = true;

        } else if (codeAndDate.isSameDate(existing)) {

            //if we already have an ethnicity code on the same date, then overwrite if it doesn't map to an ethnicity value
            EmisClinicalCode existingCode = existing.getCodeMapping();
            EthnicCategory existingEc = EmisMappingHelper.findEthnicityCode(existingCode);
            if (existingEc == null) {
                addToMap = true;
            }
        }

        if (addToMap) {
            ethnicityMap.put(key, codeAndDate);
        }
    }

    public CodeAndDate findEthnicity(CsvCell patientGuid) {
        String localUniquePatientId = createUniqueId(patientGuid, null);
        StringMemorySaver key = new StringMemorySaver(localUniquePatientId);
        return ethnicityMap.remove(key);
    }

    public void cacheMaritalStatus(CsvCell patientGuid, CodeAndDate codeAndDate) {
        String localUniquePatientId = createUniqueId(patientGuid, null);
        StringMemorySaver key = new StringMemorySaver(localUniquePatientId);
        CodeAndDate existing = maritalStatusMap.get(key);
        if (codeAndDate.isAfterOrOtherIsNull(existing)) {
            maritalStatusMap.put(key, codeAndDate);
        }
    }

    public CodeAndDate findMaritalStatus(CsvCell patientGuid) {
        String localUniquePatientId = createUniqueId(patientGuid, null);
        StringMemorySaver key = new StringMemorySaver(localUniquePatientId);
        return maritalStatusMap.remove(key);
    }

    /**
     * when the transform is complete, if there's any values left in the ethnicity and marital status maps,
     * then we need to update pre-existing patients with new data
     */
    public void processRemainingEthnicitiesAndMartialStatuses(FhirResourceFiler fhirResourceFiler) throws Exception {

        //get a combined list of the keys (patientGuids) from both maps
        HashSet<StringMemorySaver> patientGuids = new HashSet<>(ethnicityMap.keySet());
        patientGuids.addAll(new HashSet<>(maritalStatusMap.keySet()));

        for (StringMemorySaver key: patientGuids) {

            CodeAndDate newEthnicity = ethnicityMap.get(key);
            CodeAndDate newMaritalStatus = maritalStatusMap.get(key);

            String patientGuid = key.toString();

            Patient fhirPatient = (Patient)retrieveResource(patientGuid, ResourceType.Patient);
            if (fhirPatient == null) {
                //if we try to update the ethnicity on a deleted patient, or one we've never received, we'll get this exception, which is fine to ignore
                continue;
            }

            PatientBuilder patientBuilder = new PatientBuilder(fhirPatient);

            if (newEthnicity != null) {
                EmisClinicalCode codeMapping = newEthnicity.getCodeMapping();
                CsvCell[] additionalSourceCells = newEthnicity.getAdditionalSourceCells();
                EmisCodeHelper.applyEthnicity(patientBuilder, codeMapping, additionalSourceCells);
            }

            if (newMaritalStatus != null) {
                EmisClinicalCode codeMapping = newMaritalStatus.getCodeMapping();
                CsvCell[] additionalSourceCells = newMaritalStatus.getAdditionalSourceCells();
                EmisCodeHelper.applyMaritalStatus(patientBuilder, codeMapping, additionalSourceCells);
            }

            fhirResourceFiler.savePatientResource(null, false, patientBuilder);
        }
    }




    /*public void applyAdminResourceCache(FhirResourceFiler fhirResourceFiler) throws Exception {

        List<EmisAdminResourceCache> cachedResources = mappingRepository.getAdminResources(dataSharingAgreementGuid);
        LOG.trace("Got to apply " + cachedResources.size() + " admin resources");

        int count = 0;

        for (EmisAdminResourceCache cachedResource: cachedResources) {

            //wrap the resource and audit trail in a generic resource builder for saving
            Resource fhirResource = FhirSerializationHelper.deserializeResource(cachedResource.getResourceData());
            ResourceFieldMappingAudit audit = cachedResource.getAudit();
            GenericBuilder genericBuilder = new GenericBuilder(fhirResource, audit);

            fhirResourceFiler.saveAdminResource(null, genericBuilder);

            //to cut memory usage, clear out the JSON field on each object as we pass it. Due to weird
            //Emis org/practitioner hierarchy, we've got 500k practitioners to save, so this is quite a lot of memory
            cachedResource.setDataSharingAgreementGuid(null);
            cachedResource.setEmisGuid(null);
            cachedResource.setResourceType(null);
            cachedResource.setResourceData(null);
            cachedResource.setAudit(null);

            //log progress
            count ++;
            if (count % 50000 == 0) {
                LOG.trace("Done " + count + " / " + cachedResources.size());
            }
        }
    }*/

    /**
     * in some cases, we get a row in the CareRecord_Problem file but not in the CareRecord_Observation file,
     * when something about the problem only has changed (e.g. ending a problem). This is called at the end
     * of the transform to handle those changes to problems that weren't handled when we processed the Observation file.
     */
    public void processRemainingProblems(FhirResourceFiler fhirResourceFiler) throws Exception {

        //the findProblem removes from the map, so we can't iterate directly over the keySet and need to copy that into a separate collection
        //for (String uid: problemMap.keySet()) {
        List<StringMemorySaver> keys = new ArrayList<>(problemMap.keySet());
        for (StringMemorySaver key: keys) {
            ConditionBuilder conditionBuilder = problemMap.getAndRemoveFromCache(key);

            if (conditionBuilder == null) {
                throw new TransformException("Null ConditionBuilder for UID " + key);
            }

            //if the resource has the Condition profile URI, then it means we have a pre-existing problem
            //that's now been deleted from being a problem, but the root Observation itself has not (i.e.
            //the problem has been down-graded from being a problem to just an observation)
            if (conditionBuilder.isProblem()) {
                updateExistingProblem(conditionBuilder, fhirResourceFiler);

            } else {
                String uid = key.toString();
                downgradeExistingProblemToCondition(uid, fhirResourceFiler);
            }
        }
    }

    /**
     * updates an existing problem with new data we've received, when we didn't also get an update in the Observation file
     */
    private void updateExistingProblem(ConditionBuilder newConditionBuilder, FhirResourceFiler fhirResourceFiler) throws Exception {

        String locallyUniqueId = newConditionBuilder.getResourceId();

        Condition existingResource = (Condition)retrieveResource(locallyUniqueId, ResourceType.Condition);
        if (existingResource == null) {
            //emis seem to send bulk data containing deleted records, so ignore any attempt to downgrade
            //a problem that doesn't actually exist
            return;
        }

        //create a new condition builder around the old problem instance, so we can copy the new content into it
        //we also need to carry over all the audits from the part-completed problem to the new condition builder
        ResourceFieldMappingAudit newAudit = newConditionBuilder.getAuditWrapper();
        ConditionBuilder conditionBuilder = new ConditionBuilder(existingResource, newAudit);

        //we may be upgrading an existing condition to a problem, so make sure we change the profile URL
        conditionBuilder.setAsProblem(true);

        //then carry over all the new data from the updated resource
        Type endDateOrBoolean = newConditionBuilder.getEndDateOrBoolean();
        conditionBuilder.setEndDateOrBoolean(endDateOrBoolean);

        Integer duration = newConditionBuilder.getExpectedDuration();
        conditionBuilder.setExpectedDuration(duration);

        DateType lastReviewDate = newConditionBuilder.getProblemLastReviewDate();
        conditionBuilder.setProblemLastReviewDate(lastReviewDate);

        Reference lastReviewedBy = newConditionBuilder.getProblemLastReviewedBy();
        if (lastReviewedBy != null) {
            //the reference will be using an Emis guid, so map to a Discovery UUID
            Reference globallyUniqueReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(lastReviewedBy, fhirResourceFiler);
            conditionBuilder.setProblemLastReviewedBy(globallyUniqueReference);
        } else {
            conditionBuilder.setProblemLastReviewedBy(null);
        }

        ProblemSignificance problemSignificance = newConditionBuilder.getProblemSignificance();
        conditionBuilder.setProblemSignificance(problemSignificance);

        Reference parentProblem = newConditionBuilder.getParentProblem();
        if (parentProblem != null) {
            //the reference will be using an Emis guid, so map to a Discovery UUID
            Reference globallyUniqueReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(parentProblem, fhirResourceFiler);
            conditionBuilder.setParentProblem(globallyUniqueReference);
        } else {
            conditionBuilder.setParentProblem(null);
        }

        ProblemRelationshipType parentProblemRelationship = newConditionBuilder.getParentProblemRelationship();
        conditionBuilder.setParentProblemRelationship(parentProblemRelationship);

        /*String additionalNotes = newConditionBuilder.getAdditionalNotes();
        conditionBuilder.setAdditionalNotes(additionalNotes);*/

        //carry over any new links to child items
        ContainedListBuilder containedListBuilder = new ContainedListBuilder(conditionBuilder);

        ContainedListBuilder newContainedListBuilder = new ContainedListBuilder(newConditionBuilder);
        List<Reference> newLinkedItems = newContainedListBuilder.getReferences();
        for (Reference reference: newLinkedItems) {
            Reference mappedReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(reference, fhirResourceFiler);

            containedListBuilder.addReference(mappedReference);
        }

        //make sure to pass in FALSE to bypass ID mapping, since we're saving an already ID mapped instance
        fhirResourceFiler.savePatientResource(null, false, conditionBuilder);
    }

    /**
     * down-grades an existing problem to a regular condition, by changing the profile URI and removing all
     * the problem-specific data, leaving just the original condition
     */
    private void downgradeExistingProblemToCondition(String locallyUniqueId, FhirResourceFiler fhirResourceFiler) throws Exception {

        Condition existingProblem = (Condition)retrieveResource(locallyUniqueId, ResourceType.Condition);
        if (existingProblem == null) {
            //emis seem to send bulk data containing deleted records, so ignore any attempt to downgrade
            //a problem that doesn't actually exist
            return;
        }

        ConditionBuilder conditionBuilder = new ConditionBuilder(existingProblem);

        //change its profile URL to say it's not a problem
        conditionBuilder.setAsProblem(false);

        fhirResourceFiler.savePatientResource(null, false, conditionBuilder);
    }
    /*private void removeAllProblemSpecificFields(ConditionBuilder conditionBuilder) {

        Condition fhirProblem = (Condition)conditionBuilder.getResource();
        if (fhirProblem.hasAbatement()) {
            fhirProblem.setAbatement(null);
        }

        if (fhirProblem.hasExtension()) {
            List<Extension> extensions = fhirProblem.getExtension();

            //iterate backwards, so we can safely remove as we go
            for (int i=extensions.size()-1; i>=0; i--) {
                Extension extension = extensions.get(i);
                String url = extension.getUrl();
                if (url.equals(FhirExtensionUri.PROBLEM_EXPECTED_DURATION)
                        || url.equals(FhirExtensionUri.PROBLEM_LAST_REVIEWED)
                        || url.equals(FhirExtensionUri.PROBLEM_SIGNIFICANCE)
                        || url.equals(FhirExtensionUri.PROBLEM_RELATED)
                        || url.equals(FhirExtensionUri.PROBLEM_ASSOCIATED_RESOURCE)) {
                    extensions.remove(i);
                }
            }
        }

        if (fhirProblem.hasContained()) {
            for (Resource contained: fhirProblem.getContained()) {
                if (contained.getId().equals(CONTAINED_LIST_ID)) {
                    fhirProblem.getContained().remove(contained);
                    break;
                }
            }
        }
    }*/

    /*private static boolean isCondition(Condition condition) {

        Meta meta = condition.getMeta();
        for (UriType profileUri: meta.getProfile()) {
            if (profileUri.getValue().equals(FhirProfileUri.PROFILE_URI_CONDITION)) {
                return true;
            }
        }

        return false;
    }*/



    public void cacheNewConsultationChildRelationship(CsvCell consultationGuid,
                                                      CsvCell patientGuid,
                                                      CsvCell resourceGuid,
                                                      ResourceType resourceType) {

        if (consultationGuid.isEmpty()) {
            return;
        }

        String consultationLocalUniqueId = createUniqueId(patientGuid, consultationGuid);
        StringMemorySaver key = new StringMemorySaver(consultationLocalUniqueId);
        ReferenceList list = consultationNewChildMap.get(key);
        if (list == null) {
            list = new ReferenceListSingleCsvCells(); //we know there will only be a single cell, so use this reference list class to save memory

            consultationNewChildMap.put(key, list);
        }

        String resourceLocalUniqueId = createUniqueId(patientGuid, resourceGuid);
        list.add(resourceType, resourceLocalUniqueId, consultationGuid);
    }

    public ReferenceList getAndRemoveNewConsultationRelationships(String encounterSourceId) {
        StringMemorySaver key = new StringMemorySaver(encounterSourceId);
        return consultationNewChildMap.remove(key);
    }

    public void processRemainingNewConsultationRelationships(FhirResourceFiler fhirResourceFiler) throws Exception {
        for (StringMemorySaver key: consultationNewChildMap.keySet()) {
            ReferenceList newLinkedItems = consultationNewChildMap.get(key);

            String locallyUniqueResourceEncounterId = key.toString();
            Encounter existingEncounter = (Encounter)retrieveResource(locallyUniqueResourceEncounterId, ResourceType.Encounter);
            if (existingEncounter == null) {
                //if the problem has been deleted, just skip it
                return;
            }

            EncounterBuilder encounterBuilder = new EncounterBuilder(existingEncounter);
            ContainedListBuilder containedListBuilder = new ContainedListBuilder(encounterBuilder);

            for (int i=0; i<newLinkedItems.size(); i++) {
                Reference reference = newLinkedItems.getReference(i);
                CsvCell[] sourceCells = newLinkedItems.getSourceCells(i);

                Reference globallyUniqueReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(reference, fhirResourceFiler);
                containedListBuilder.addReference(globallyUniqueReference, sourceCells);
            }

            fhirResourceFiler.savePatientResource(null, false, encounterBuilder);
        }
    }



    public void cacheProblemObservationGuid(CsvCell patientGuid, CsvCell problemGuid, String readCode) {
        String uid = createUniqueId(patientGuid, problemGuid);
        StringMemorySaver key = new StringMemorySaver(uid);
        problemReadCodes.put(key, readCode);
    }

    public boolean isProblemObservationGuid(CsvCell patientGuid, CsvCell problemGuid) {
        String uid = createUniqueId(patientGuid, problemGuid);
        StringMemorySaver key = new StringMemorySaver(uid);
        return problemReadCodes.containsKey(key);
    }

    public String findProblemObservationReadCode(CsvCell patientGuid, CsvCell problemGuid) throws Exception {

        String locallyUniqueId = createUniqueId(patientGuid, problemGuid);
        StringMemorySaver key = new StringMemorySaver(locallyUniqueId);

        //if we've already cached our problem code, then just return it
        if (problemReadCodes.containsKey(key)) {
            return problemReadCodes.get(key);
        }

        //if we've not cached our problem code, then the problem itself isn't part of this extract,
        //so we'll need to retrieve it from the DB and cache the code
        String readCode = null;

        Condition fhirPproblem = (Condition)retrieveResource(locallyUniqueId, ResourceType.Condition);

        //we've had cases of data referring to non-existent problems, so check for null
        if (fhirPproblem != null) {
            CodeableConcept codeableConcept = fhirPproblem.getCode();
            readCode = CodeableConceptHelper.findOriginalCode(codeableConcept);
        }

        problemReadCodes.put(key, readCode);
        return readCode;
    }

    /**
     * when we process the IssueRecord file, we build up a map of first and last issue dates for the medication.
     * If we have any entries in the maps at the end of the transform, then we need to update the existing
     * resources on the DB with these new dates
     */
    public void processRemainingMedicationIssueDates(FhirResourceFiler fhirResourceFiler) throws Exception {

        //both maps (first and last issue dates) will have the same key set
        for (StringMemorySaver key: drugRecordNewLastIssueDateMap.keySet()) {

            String medicationStatementLocalId = key.toString();
            MedicationStatement fhirMedicationStatement = (MedicationStatement)retrieveResource(medicationStatementLocalId, ResourceType.MedicationStatement);
            if (fhirMedicationStatement == null) {
                //if the medication statement doesn't exist or has been deleted, then just skip it
                continue;
            }

            IssueRecordIssueDate newFirstIssueDate = drugRecordNewFirstIssueDateMap.get(key);
            IssueRecordIssueDate newLastIssueDate = drugRecordNewLastIssueDateMap.get(key);

            MedicationStatementBuilder medicationStatementBuilder = new MedicationStatementBuilder(fhirMedicationStatement);
            boolean changed = false;

            //only set if new one is earlier
            DateType existingFirstIssueDate = ExtensionConverter.findExtensionValueDate(fhirMedicationStatement, FhirExtensionUri.MEDICATION_AUTHORISATION_FIRST_ISSUE_DATE);
            if (newFirstIssueDate.beforeOrOtherIsNull(existingFirstIssueDate)) {
                medicationStatementBuilder.setFirstIssueDate(newFirstIssueDate.getIssueDateType(), newFirstIssueDate.getSourceCells());
                changed = true;
            }

            //only set if new one is later
            DateType existingLastIssueDate = ExtensionConverter.findExtensionValueDate(fhirMedicationStatement, FhirExtensionUri.MEDICATION_AUTHORISATION_MOST_RECENT_ISSUE_DATE);
            if (newFirstIssueDate.afterOrOtherIsNull(existingLastIssueDate)) {
                medicationStatementBuilder.setLastIssueDate(newLastIssueDate.getIssueDateType(), newLastIssueDate.getSourceCells());
                changed = true;
            }

            //save, making sure to skip ID mapping (since it's already mapped)
            if (changed) {
                //String patientGuid = getPatientGuidFromUniqueId(medicationStatementLocalId);
                fhirResourceFiler.savePatientResource(null, false, medicationStatementBuilder);
            }
        }
    }

    public void cacheConsultationPreviousLinkedResources(String encounterSourceId, List<Reference> previousReferences) {

        if (previousReferences == null
                || previousReferences.isEmpty()) {
            return;
        }

        //we know there will be no CsvCells, so use this reference list class to save memory
        ReferenceList obj = new ReferenceListNoCsvCells();
        //ReferenceList obj = new ReferenceList();
        obj.add(previousReferences);

        StringMemorySaver key = new StringMemorySaver(encounterSourceId);
        consultationExistingChildMap.put(key, obj);
    }

    public ReferenceList findConsultationPreviousLinkedResources(String encounterSourceId) {
        StringMemorySaver key = new StringMemorySaver(encounterSourceId);
        return consultationExistingChildMap.remove(key);
    }


    public void cacheProblemPreviousLinkedResources(String problemSourceId, List<Reference> previousReferences) {
        if (previousReferences == null
                || previousReferences.isEmpty()) {
            return;
        }

        StringMemorySaver key = new StringMemorySaver(problemSourceId);

        //we know there will be no CsvCells, so use this reference list class to save memory
        ReferenceList obj = new ReferenceListNoCsvCells();
        //ReferenceList obj = new ReferenceList();
        obj.add(previousReferences);

        problemPreviousLinkedResources.put(key, obj);
    }

    public ReferenceList findProblemPreviousLinkedResources(String problemSourceId) {
        StringMemorySaver key = new StringMemorySaver(problemSourceId);
        return problemPreviousLinkedResources.remove(key);
    }


    public ResourceType getCachedParentObservationResourceType(CsvCell patientGuidCell, CsvCell parentObservationCell) {
        String locallyUniqueId = createUniqueId(patientGuidCell, parentObservationCell);
        StringMemorySaver key = new StringMemorySaver(locallyUniqueId);
        return parentObservationResourceTypes.get(key); //note we specifically don't remove from here as multiple obs link to a single parent
    }

    public void cacheParentObservationResourceType(CsvCell patientGuidCell, CsvCell observationGuidCell, ResourceType resourceType) {
        String locallyUniqueId = createUniqueId(patientGuidCell, observationGuidCell);
        StringMemorySaver key = new StringMemorySaver(locallyUniqueId);
        parentObservationResourceTypes.put(key, resourceType);
    }

    /**
     * we've cached the resource type of EVERY observation, but only need those with a parent-child link,
     * so call this to remove the unnecessary ones and free up some memory
     */
    public void pruneUnnecessaryParentObservationResourceTypes() {

        //just removing the unnecessary keys from the map doesn't shrink the map back down,
        //so it ends up chewing memory up. So recreate the map, so keep it leaner
        Map<StringMemorySaver, ResourceType> tmp = new HashMap<>();

        for (StringMemorySaver key: observationChildMap.keySet()) {
            ResourceType cachedType = parentObservationResourceTypes.get(key);
            if (cachedType != null) {
                tmp.put(key, cachedType);
            }
        }

        this.parentObservationResourceTypes = tmp;

    }

    public void submitToThreadPool(Callable callable) throws Exception {
        if (this.utilityThreadPool == null) {
            int threadPoolSize = ConnectionManager.getPublisherTransformConnectionPoolMaxSize(serviceId);
            this.utilityThreadPool = new ThreadPool(threadPoolSize, 1000, "EmisCsvHelper"); //lower from 50k to save memory
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

    public void cacheExistingRegistrationStatuses(String sourceId, List<List_.ListEntryComponent> items) {
        StringMemorySaver key = new StringMemorySaver(sourceId);
        existingRegsitrationStatues.put(key, items);
    }

    public List<List_.ListEntryComponent> getExistingRegistrationStatuses(String sourceId) {
        StringMemorySaver key = new StringMemorySaver(sourceId);
        return existingRegsitrationStatues.get(key);
    }


    public boolean allowedToProcessedDisabledExtract() throws Exception {

        ServiceDalI serviceDal = DalProvider.factoryServiceDal();
        Service service = serviceDal.getById(this.serviceId);
        String odsCode = service.getLocalId();

        Set<String> disabledOrgIdsAllowed = TransformConfig.instance().getEmisDisabledOdsCodesAllowed();
        return disabledOrgIdsAllowed.contains(odsCode);
    }

    public boolean isSharingAgreementDisabled() {
        if (sharingAgreementDisabled == null) {
            throw new RuntimeException("Have missed processing sharing agreement");
        }
        return sharingAgreementDisabled.booleanValue();
    }

    public void setSharingAgreementDisabled(boolean sharingAgreementDisabled) {
        this.sharingAgreementDisabled = new Boolean(sharingAgreementDisabled);
    }


    /**
     * To log the missing Clinical/Drug codeIds into the table.
     */
    public void logMissingCode(EmisCodeNotFoundException rex, CsvCell patientRecordGuid, CsvCell recordGuid, AbstractCsvParser parser) throws Exception {

        EmisMissingCodes errorCodeObj = new EmisMissingCodes();
        errorCodeObj.setCodeId(rex.getCode());
        errorCodeObj.setExchangeId(getExchangeId());
        errorCodeObj.setServiceId(getServiceId());
        errorCodeObj.setPatientGuid(patientRecordGuid.getString());
        errorCodeObj.setRecordGuid(recordGuid.getString());
        errorCodeObj.setFileType(parser.getClass().getSimpleName());
        errorCodeObj.setCodeType(rex.getCodeType());

        //if it will be a new code, send an alert to tell us
        EmisMissingCodeDalI dal = DalProvider.factoryEmisMissingCodeDal();
        if (!dal.isCurrentMissingCode(errorCodeObj)) {
            ServiceDalI serviceDalI = DalProvider.factoryServiceDal();
            Service service = serviceDalI.getById(serviceId);

            String msg = "New Emis missing code (" + errorCodeObj.getCodeId() + ") found in " + errorCodeObj.getFileType() + " file "
                    + "in Exchange " + exchangeId + " for " + service.getName() + " " + service.getLocalId();
            SlackHelper.sendSlackMessage(SlackHelper.Channel.QueueReaderAlerts, msg);
        }

        //and save to the DB
        dal.saveMissingCodeError(errorCodeObj);
    }


    public Set<Long> retrieveMissingCodes(EmisCodeType emisCodeType) throws Exception {
        EmisMissingCodeDalI dal = DalProvider.factoryEmisMissingCodeDal();
        return dal.retrieveMissingCodes(emisCodeType, getServiceId());
    }

    public void addFoundMissingCodes(Set<Long> codes) {
        this.foundMissingCodes.addAll(codes);
    }

    /**
     * when we've detected that missing codes have finally been received, then we re-queue all exchanges since
     * the first error back into RabbitMQ for re-processing, with a special header to filter the processing
     * by just the affected PatientGuids.
     * The re-queuing is done by calling the DDS-UI ExchangeAuditEndpoint class
     */
    public void queueExchangesForFoundMissingCodes() throws Exception {

        //if we've not found any missing codes, there's nothing to do
        if (this.foundMissingCodes.isEmpty()) {
            return;
        }

        LOG.debug("Found " + this.foundMissingCodes.size() + " missing codes");

        //find the patient GUIDs affected
        EmisMissingCodeDalI dal = DalProvider.factoryEmisMissingCodeDal();
        Set<String> patientGuidSet = dal.retrievePatientGuidsForMissingCodes(this.foundMissingCodes, getServiceId());
        LOG.debug("Found " + patientGuidSet.size() + " patients to re-queue");

        //find the exchange IDs we'll need to re-queue
        UUID firstExchangeId = dal.retrieveOldestExchangeIdForMissingCodes(this.foundMissingCodes, getServiceId());

        Service service = DalProvider.factoryServiceDal().getById(getServiceId());
        String msg = "" + this.foundMissingCodes.size() + " missing codes have been found for " + service.getLocalId() + " " + service.getName() + "\r\n"
                + "Exchanges from " + firstExchangeId + " will be re-queued, filtering on " + patientGuidSet.size() + " patientGUIDs";
        SlackHelper.sendSlackMessage(SlackHelper.Channel.QueueReaderAlerts, msg);

        //get the Keycloak details from the EMIS config
        JsonNode json = ConfigManager.getConfigurationAsJson("emis_config", "queuereader");
        json = json.get("missing_code_fix");

        String ddsUrl = json.get("dds-ui-url").asText();
        String keyCloakUrl = json.get("keycloak-url").asText();
        String keyCloakRealm = json.get("keycloak-realm").asText();
        String keyCloakUser = json.get("keycloak-username").asText();
        String keyCloakPass = json.get("keycloak-password").asText();
        String keyCloakClientId = json.get("keycloak-client-id").asText();
        int batchSize = json.get("max_batch_size").asInt(); //do 3000 at a time

        KeycloakClient kcClient = new KeycloakClient(keyCloakUrl, keyCloakRealm, keyCloakUser, keyCloakPass, keyCloakClientId);

        //if we try to post with more than about 3000 PatientGUIDs, RabbitMQ reject the post, because it's too large (looks like the limit is about 128KB)
        //so we need to break into blocks of 3000 patients
        List<String> patientGuidList = new ArrayList<>(patientGuidSet);
        while (!patientGuidList.isEmpty()) {

            List<String> patientGuidBatch = new ArrayList<>();
            while (patientGuidBatch.size() < batchSize && !patientGuidList.isEmpty()) {
                String guid = patientGuidList.remove(0);
                patientGuidBatch.add(guid);
            }

            //note the below parameters match the expected JSON received in ExchangeAuditEndpoint.postToExchange(..)
            String[] patientGuidArr = patientGuidBatch.toArray(new String[0]);
            String patientGuidHeaderValue = ObjectMapperPool.getInstance().writeValueAsString(patientGuidArr);

            ObjectMapper mapper = new ObjectMapper();
            ObjectNode jsonRequestRoot = new ObjectNode(mapper.getNodeFactory());

            jsonRequestRoot.put("exchangeId", firstExchangeId.toString());
            jsonRequestRoot.put("serviceId", getServiceId().toString());
            jsonRequestRoot.put("systemId", getSystemId().toString());
            jsonRequestRoot.put("exchangeName", "EdsInbound"); //want to target the inbound queue
            jsonRequestRoot.put("postMode", "Onwards"); //want to queue from our first exchange ID and onward
            jsonRequestRoot.put("reason", "Missing codes found for " + patientGuidBatch.size() + " patients"); //free-text reason for re-queuing
            ObjectNode subObj = jsonRequestRoot.putObject("additionalHeaders");
            subObj.put(HeaderKeys.EmisPatientGuids, patientGuidHeaderValue);

            String parameters = mapper.writeValueAsString(jsonRequestRoot);


            //use a slightly different URL now so that we can use a user from the machine realm in Keycloak
            //TODO - finish tesitng this and get it using the new Keycloak user and different API
            WebTarget target = ClientBuilder.newClient().target(ddsUrl).path("api/exchangeAudit/postToExchange");
            //WebTarget target = ClientBuilder.newClient().target(ddsUrl).path("api/exchangeAudit/PostToExchangeApi");


            Response response = target
                    .request()
                    .header("Authorization", "Bearer " + kcClient.getToken().getToken())
                    .post(Entity.entity(parameters, MediaType.APPLICATION_JSON));

            if (response.getStatus() != HttpStatus.SC_OK) {
                throw new Exception("Failed to re-queue Emis exchanges from exchange " + firstExchangeId + " due to found missing codes with HTTP response " + response.getStatus());
            }
        }

        //update the audit DB to say we've handled these missing codes
        LOG.info("Re-queued exchanges for " + patientGuidSet.size() + " patients for fixed missing codes");
        dal.setMissingCodesFixed(this.foundMissingCodes, getServiceId());
    }

    public EmisPatientFiler getPatientFilter() {
        return patientFilter;
    }

    public void setPatientFilter(EmisPatientFiler patientFilter) {
        this.patientFilter = patientFilter;
    }

    public boolean shouldProcessRecord(AbstractCsvParser parser) {
        return this.patientFilter.shouldProcessRecord(parser);
    }

    public EmisAdminHelper getAdminHelper() {
        return adminHelper;
    }

    /*
    //Combine both the matched CLinical and Drug Codes
    public List<String> retrieveEmisCombineClinicalDrugCodes() {

        if (emisMissingClinicalCodesMatch != null && emisMissingClinicalCodesMatch.size() > 0) {
            emisMissingClinicalCodesMatch.addAll(emisMissingDrugCodesMatch);
        } else {
            emisMissingClinicalCodesMatch.addAll(emisMissingDrugCodesMatch);
        }
        return emisMissingClinicalCodesMatch;
    }


    public void setEmisMissingClinicalCodesMatch(List<String> emisMissingClinicalCodesMatch) {
        this.emisMissingClinicalCodesMatch = emisMissingClinicalCodesMatch;
    }
    public String getEmisMissingPatientGuids() {
        return emisMissingPatientGuids;
    }

    public void setEmisMissingPatientGuids(String emisMissingPatientGuids) {
        this.emisMissingPatientGuids = emisMissingPatientGuids;
    }
    public void setEmisMissingDrugCodesMatch(List<String> emisMissingDrugCodesMatch) {
        this.emisMissingDrugCodesMatch = emisMissingDrugCodesMatch;
    }

    //Requeue the missing patientGuids by invoking the rest method.
    public Response reprocessEmisPatientGuids(List<String> emisPatientGuIds, List<String> emisAllExchangeIds) throws Exception {


    }


    *//*
    Pass the latestExchangeId and get the latest timestamp and in the same way pass the OldestExchangeId and get the OldestTimeStamp,
    Outcome of this method is to  get all the exchangeIds from the date Error logged in the EmisMissingCodeError table to the latest date in the Exchange Table.
     *//*
    public List<String> retrieveEmisAllExchangeIds(String emisOldestExchangeId, String emisLatestExchangeId) throws Exception {

        List<Exchange> emisExchangeIdsFrToDate = new ArrayList<Exchange>();
        List<String> emisAllExchangeIds = new ArrayList<>();
        ExchangeDalI auditExchangeDal = DalProvider.factoryExchangeDal();
        Exchange exchangeObjOld = auditExchangeDal.getExchange(UUID.fromString(emisOldestExchangeId));
        Date emisOldestTimeStamp = exchangeObjOld.getTimestamp();
        Exchange exchangeObjLatest = auditExchangeDal.getExchange(UUID.fromString(emisLatestExchangeId));
        Date emisLatestTimeStamp = exchangeObjLatest.getTimestamp();
        emisExchangeIdsFrToDate = auditExchangeDal.getExchangesByService(getServiceId(), getSystemId(), 10000, emisOldestTimeStamp, emisLatestTimeStamp);
        if(emisExchangeIdsFrToDate!=null && emisExchangeIdsFrToDate.size()>0){
            for(Exchange exchangeIds : emisExchangeIdsFrToDate){
                emisAllExchangeIds.add(exchangeIds.getId().toString());
            }
        }

        return emisAllExchangeIds;
    }
    */
}

