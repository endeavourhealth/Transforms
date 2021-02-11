package org.endeavourhealth.transform.subscriber;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.utility.ExpiringCache;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.eds.PatientLinkDalI;
import org.endeavourhealth.core.database.dal.eds.PatientSearchDalI;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.reference.SnomedToBnfChapterDalI;
import org.endeavourhealth.core.database.dal.subscriberTransform.ExchangeBatchExtraResourceDalI;
import org.endeavourhealth.core.database.dal.subscriberTransform.SubscriberPatientDateDalI;
import org.endeavourhealth.core.database.dal.subscriberTransform.SubscriberPersonMappingDalI;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirResourceHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.HasServiceSystemAndExchangeIdI;
import org.endeavourhealth.transform.common.ResourceParser;
import org.endeavourhealth.transform.subscriber.targetTables.OutputContainer;
import org.endeavourhealth.transform.subscriber.targetTables.SubscriberTableId;
import org.endeavourhealth.transform.subscriber.transforms.AbstractSubscriberTransformer;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

public class SubscriberTransformHelper implements HasServiceSystemAndExchangeIdI {
    private static final Logger LOG = LoggerFactory.getLogger(SubscriberTransformHelper.class);

    private static final long CACHE_DURATION = 1000 * 60 * 60; //cache objects for 2hrs
    private static final Object MAP_VAL = new Object(); //concurrent hash map requires non-null values, so just use this

    private static Set<String> protectedCodesRead2;
    private static Set<String> protectedCodesCTV3;
    private static Set<String> protectedCodesSnomed;

    private final UUID serviceId;
    private final UUID systemId;
    private final UUID exchangeId;
    private final UUID batchId;
    private final OutputContainer outputContainer;
    //private final String subscriberConfigName;
    //private final boolean isPseudonymised;
    //private final boolean includeDateRecorded;
    //private final boolean hasEncounterEventTable;
    //private final int batchSize;
    //private final String excludeNhsNumberRegex;
    private final Map<String, ResourceWrapper> hmAllResourcesByReferenceString;
    private final List<ResourceWrapper> allResources;
    private final Map<String, Object> resourcesTransformedReferences = new ConcurrentHashMap<>(); //treated as a set, but need concurrent access
    private final Map<String, Object> resourcesSkippedReferences = new ConcurrentHashMap<>(); //treated as a set, but need concurrent access
    private final SubscriberConfig config;
    private final boolean isBulkDeleteFromSubscriber;
    private Boolean shouldPatientRecordBeDeleted = null; //whether the record should exist in the enterprise DB (e.g. if confidential)
    private Patient cachedPatient = null;
    private static ExpiringCache<String, String> snomedToBnfChapter = new ExpiringCache<>(CACHE_DURATION);
    private final ReentrantLock lock = new ReentrantLock();
    private ResourceWrapper patientTransformedWrapper = null;

    private Long subscriberOrganisationId = null;
    private Long subscriberPatientId = null;
    private Long subscriberPersonId = null;

    public SubscriberTransformHelper(UUID serviceId, UUID systemId, UUID exchangeId, UUID batchId, SubscriberConfig subscriberConfig,
                                     List<ResourceWrapper> allResources, boolean isBulkDeleteFromSubscriber) throws Exception {

        this(serviceId, systemId, exchangeId, batchId, subscriberConfig, allResources, isBulkDeleteFromSubscriber, new OutputContainer());
    }

    public SubscriberTransformHelper(UUID serviceId, UUID systemId, UUID exchangeId, UUID batchId, SubscriberConfig subscriberConfig,
                List<ResourceWrapper> allResources, boolean isBulkDeleteFromSubscriber, OutputContainer outputContainer) throws Exception {
        this.serviceId = serviceId;
        this.systemId = systemId;
        this.exchangeId = exchangeId;
        this.batchId = batchId;
        this.outputContainer = outputContainer;
        this.isBulkDeleteFromSubscriber = isBulkDeleteFromSubscriber;

        //load our config record for some parameters
        this.config = subscriberConfig;
        if (config.getSubscriberType() != SubscriberConfig.SubscriberType.CompassV2) {
            throw new Exception("Expecting config for compassv2 but got " + config.getSubscriberType());
        }

        //hash the resources by reference to them, so the transforms can quickly look up dependant resources
        this.allResources = allResources;
        this.hmAllResourcesByReferenceString = hashResourcesByReference(allResources);
    }

    /**
     * hashes the resources by a reference to them, so the transforms can quickly look up dependant resources
     */
    private static Map<String, ResourceWrapper> hashResourcesByReference(List<ResourceWrapper> l) throws Exception {

        Map<String, ResourceWrapper> ret = new HashMap<>();

        for (ResourceWrapper resource : l) {

            ResourceType resourceType = ResourceType.valueOf(resource.getResourceType());
            String resourceId = resource.getResourceId().toString();

            Reference reference = ReferenceHelper.createReference(resourceType, resourceId);
            String referenceStr = reference.getReference();
            if (ret.containsKey(referenceStr)) {
                LOG.warn("Duplicate resource found " + referenceStr);
            }
            ret.put(referenceStr, resource);
        }

        return ret;
    }

    public boolean isPseudonymised() {
        return config.isPseudonymised();
    }

    public boolean isIncludeDateRecorded() {
        return config.isIncludeDateRecorded();
    }

    public boolean isIncludeObservationAdditional() {
        return config.isIncludeObservationAdditional();
    }

    public boolean isBulkDeleteFromSubscriber() {
        return isBulkDeleteFromSubscriber;
    }

    public Date includeDateRecorded(DomainResource fhir) {
        Date dateRecorded = null;
        if (isIncludeDateRecorded()) {
            Extension recordedDate = ExtensionConverter.findExtension(fhir, FhirExtensionUri.RECORDED_DATE);
            if (recordedDate != null) {
                DateTimeType value = (DateTimeType) recordedDate.getValue();
                if (value.getValue() != null) {
                    dateRecorded = value.getValue();
                }
            }
        }
        return dateRecorded;
    }

     public boolean isIncludePatientAge() {
        return config.isIncludePatientAge();
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

    public UUID getBatchId() {
        return batchId;
    }

    public String getSubscriberConfigName() {
        return config.getSubscriberConfigName();
    }

    public OutputContainer getOutputContainer() {
        return outputContainer;
    }

    public int getBatchSize() {
        return config.getBatchSize();
    }


    public Long getSubscriberOrganisationId() {
        if (subscriberOrganisationId == null) {
            throw new RuntimeException("Trying to get subscriber org ID but it's not been set");
        }
        return subscriberOrganisationId;
    }

    public void setSubscriberOrganisationId(Long subscriberOrganisationId) {
        if (this.subscriberOrganisationId != null) {
            throw new IllegalArgumentException("Cannot change the subscriberOrganisationId once set");
        }
        this.subscriberOrganisationId = subscriberOrganisationId;
    }

    public Long getSubscriberPatientId() {
        if (subscriberPatientId == null) {
            throw new RuntimeException("Trying to get subscriber patient ID but it's not been set");
        }
        return subscriberPatientId;
    }

    public void setSubscriberPatientId(Long subscriberPatientId) {
        if (this.subscriberPatientId != null) {
            throw new IllegalArgumentException("Cannot change the subscriberPatientId once set");
        }
        this.subscriberPatientId = subscriberPatientId;
    }

    public Long getSubscriberPersonId() {
        if (subscriberPersonId == null) {
            throw new RuntimeException("Trying to get subscriber person ID but it's not been set");
        }
        return subscriberPersonId;
    }

    public void setSubscriberPersonId(Long subscriberPersonId) {
        if (this.subscriberPersonId != null) {
            throw new IllegalArgumentException("Cannot change the subscriberPersonId once set");
        }
        this.subscriberPersonId = subscriberPersonId;
    }

    public boolean hasResourceBeenTransformedAddIfNot(ResourceWrapper resourceWrapper, SubscriberId subscriberId) {
        String referenceVal = ReferenceHelper.createResourceReference(resourceWrapper.getResourceType(), resourceWrapper.getResourceIdStr());
        boolean done = this.resourcesTransformedReferences.containsKey(referenceVal);

        if (!done) {
            this.resourcesTransformedReferences.put(referenceVal, MAP_VAL);
        }

        //and update the subscriber ID with the date of the version we're transforming
        //setSubscriberIdTransformed(resourceWrapper, subscriberId);

        return done;
    }



    /*public void setSubscriberIdTransformed(ResourceWrapper resourceWrapper, SubscriberId subscriberId) {

        //we need to copy the subscriber ID object otherwise when we set setDtUpdatedPreviouslySent on it, it'll
        //look like we've already sent over the current version when getting the previous version in the PatientTransformer
        Date dtCurrentVersion = resourceWrapper.getCreatedAt(); //note: this may be null if we've deleted the resource
        SubscriberId copy = new SubscriberId(subscriberId.getSubscriberTable(), subscriberId.getSubscriberId(), subscriberId.getSourceId(), dtCurrentVersion);

        try {
            //called from multiple threads, so need to lock
            lock.lock();
            subscriberIdsUpdated.add(copy);
        } finally {
            lock.unlock();
        }
    }

    public List<SubscriberId> getSubscriberIdsUpdated() {
        return subscriberIdsUpdated;
    }*/

    public int getResourceCount() {
        //no locking required as this is only called when transform is single-threaded
        return allResources.size();
    }

    /**
     * all resources in a batch are for the same patient (or no patient at all), so rather than looking
     * up the Enterprise patient ID for each resource, we can do it once at the start. To do that
     * we need the Discovery patient ID from one of the resources.
     */
    private String findPatientIdFromResources() throws Exception {

        //no locking required as this is only called when transform is single-threaded
        for (ResourceWrapper resourceWrapper : allResources) {

            ResourceType resourceType = resourceWrapper.getResourceTypeObj();
            if (!FhirResourceFiler.isPatientResource(resourceType)) {
                continue;
            }

            if (resourceWrapper.getPatientId() != null) {
                return resourceWrapper.getPatientId().toString();
            }
        }

        return null;
    }

    public void populatePatientAndPersonIds() throws Exception {

        String discoveryPatientId = findPatientIdFromResources();
        if (Strings.isNullOrEmpty(discoveryPatientId)) {
            //if no patient ID there are no patient-related resources to do
            return;
        }

        this.shouldPatientRecordBeDeleted = Boolean.FALSE;

                //retrieve the patient resource to see if the record has been deleted or is confidential
        Reference patientRef = ReferenceHelper.createReference(ResourceType.Patient, discoveryPatientId);
        ResourceWrapper patientWrapper = findOrRetrieveResourceWrapper(patientRef);
        if (patientWrapper != null) {
            this.cachedPatient = (Patient) patientWrapper.getResource();
        }

        //if our patient resource has been deleted, then everything should be deleted
        if (this.cachedPatient == null) {
            this.shouldPatientRecordBeDeleted = Boolean.TRUE;
        }

        //protocol QR now checks to see if our patient is in or out
        //this.shouldPatientRecordBeDeleted = new Boolean(!shouldPatientBePresentInSubscriber(cachedPatient));

        PatientLinkDalI patientLinkDal = DalProvider.factoryPatientLinkDal();
        String discoveryPersonId = patientLinkDal.getPersonId(discoveryPatientId);
        if (Strings.isNullOrEmpty(discoveryPersonId)) {
            //if we've got some cases where we've got a deleted patient but non-deleted patient-related resources
            //all in the same batch, because Emis sent it like that. In that case we won't have a person ID, so
            //return out without processing any of the remaining resources, since they're for a deleted patient.
            this.shouldPatientRecordBeDeleted = Boolean.TRUE;
        } else {
            SubscriberPersonMappingDalI personMappingDal = DalProvider.factorySubscriberPersonMappingDal(getSubscriberConfigName());
            this.subscriberPersonId = personMappingDal.findOrCreateEnterprisePersonId(discoveryPersonId);
        }


        String sourceId = ReferenceHelper.createResourceReference(ResourceType.Patient.toString(), discoveryPatientId);
        SubscriberId subscriberId = AbstractSubscriberTransformer.findSubscriberId(this, SubscriberTableId.PATIENT, sourceId);
        if (subscriberId == null) {
            //if we have no patient ID yet, then it's because our Patient resource has never been through this transform,
            //possibly because the patient has just entered the protocol cohort. The protocol Queue Reader
            //should detect that in the future, but for now, we need to simply force through the transform for the other resources now
            UUID patientUuid = UUID.fromString(discoveryPatientId);
            ResourceDalI resourceDal = DalProvider.factoryResourceDal();
            List<ResourceWrapper> allPatientResources = resourceDal.getResourcesByPatient(serviceId, patientUuid);
            for (ResourceWrapper wrapper : allPatientResources) {

                //add the resource so it gets transformed
                addResourceToTransform(wrapper);

                //if the patient record, we need to manually invoke the transform because this fn is only
                //called AFTER the patient has been transformed
                if (wrapper.getResourceTypeObj() == ResourceType.Patient) {
                    AbstractSubscriberTransformer transformer = FhirToSubscriberCsvTransformer.createTransformerForResourceType(ResourceType.Patient);
                    List<ResourceWrapper> l = new ArrayList<>();
                    l.add(wrapper);
                    transformer.transformResources(l, this);
                }
            }

            //now look for the enterprise patient ID again, if still null it means our patient was deleted (or never was received),
            //so set the boolean to delete everything from our subscriber DB
            subscriberId = AbstractSubscriberTransformer.findSubscriberId(this, SubscriberTableId.PATIENT, sourceId);
            if (subscriberId == null) {
                LOG.warn("No subscriber patient ID for patient " + discoveryPatientId + " so will delete everything");
                this.shouldPatientRecordBeDeleted = Boolean.TRUE;
            }
        }
        if (subscriberId != null) {
            this.subscriberPatientId = new Long(subscriberId.getSubscriberId());
        }

        //if doing a bulk delete, overwrite whatever we've calculated above
        if (isBulkDeleteFromSubscriber) {
            this.shouldPatientRecordBeDeleted = Boolean.TRUE;
        }
    }

    /*public boolean shouldPatientBePresentInSubscriber(Patient patient) {
        return shouldPatientBePresentInSubscriber(patient, config.getExcludeNhsNumberRegex());
    }*/

    /**
     * used by both FHIR->Subscriber and FHIR->Enterprise transforms
     * Aligned (as of 2019/09/18) as per Kambiz's email:
     * Confidential – include
     * No NHS number – include
     * Non-valid NHS – include
     * Dummy patients and test patients – exclude
     */
    /*public static boolean shouldPatientBePresentInSubscriber(Patient patient, String excludeNhsNumberRegex) {

        //deleted records shouldn't be in subscriber DBs
        if (patient == null) {
            return false;
        }

        //exclude test patients
        BooleanType isTestPatient = (BooleanType) ExtensionConverter.findExtensionValue(patient, FhirExtensionUri.PATIENT_IS_TEST_PATIENT);
        if (isTestPatient != null
                && isTestPatient.hasValue()
                && isTestPatient.getValue().booleanValue()) {
            return false;
        }

        //exclude the EDI test patients using regex, so it can be configured per subscriber
        //NOTE: changing the configured regex will not affect any data already in (or not in) a subscriber,
        //so that will need to be manually resolved
        if (!Strings.isNullOrEmpty(excludeNhsNumberRegex)) {
            String nhsNumber = IdentifierHelper.findNhsNumber(patient);
            if (!Strings.isNullOrEmpty(nhsNumber)
                    && Pattern.matches(excludeNhsNumberRegex, nhsNumber)) {
                return false;
            }
        }

        return true;
    }*/

    public boolean getShouldPatientRecordBeDeleted() {
        if (shouldPatientRecordBeDeleted == null) {
            throw new RuntimeException("Null shouldPatientRecordBeDeleted variable - this should always have been set");
        }
        return shouldPatientRecordBeDeleted.booleanValue();
    }

    public void addResourceToTransform(ResourceWrapper wrapper) throws Exception {

        String reference = ReferenceHelper.createResourceReference(wrapper.getResourceType(), wrapper.getResourceIdStr());

        try {
            lock.lock();

            //see if this resource is already in our list of things to transform
            if (this.hmAllResourcesByReferenceString.containsKey(reference)) {
                return;
            }
        } finally {
            lock.unlock();
        }

        //if we passed the above check, then audit that the resource was added and add to the map
        ExchangeBatchExtraResourceDalI exchangeBatchExtraResourceDalI = DalProvider.factoryExchangeBatchExtraResourceDal(getSubscriberConfigName());
        ResourceType type = wrapper.getResourceTypeObj();
        UUID resourceId = wrapper.getResourceId();
        exchangeBatchExtraResourceDalI.saveExtraResource(getExchangeId(), getBatchId(), type, resourceId);

        //and add the resource to be transformed
        try {
            lock.lock();

            this.allResources.add(wrapper);
            this.hmAllResourcesByReferenceString.put(reference, wrapper);
        } finally {
            lock.unlock();
        }
    }

    public void checkForMissedResources() throws Exception {

        List<String> missedResources = new ArrayList<>();
        int countMissedResources = 0;

        //no locking required as this is only called when transform is single-threaded
        for (String referenceStr : hmAllResourcesByReferenceString.keySet()) {
            if (!resourcesTransformedReferences.containsKey(referenceStr)
                    && !resourcesSkippedReferences.containsKey(referenceStr)) {

                countMissedResources++;
                if (missedResources.size() < 10) {
                    missedResources.add(referenceStr);
                }
            }
        }

        if (!missedResources.isEmpty()) {

            LOG.debug("Now got list size " + allResources.size() + " and map size " + hmAllResourcesByReferenceString.size());
            dumpResourceCountsByType();

            throw new TransformException("Transform to Enterprise didn't do " + countMissedResources + " resource(s). First 10 are: " + missedResources);
        }

    }

    private void dumpResourceCountsByType() {
        Map<String, List<ResourceWrapper>> hmByType = new HashMap<>();

        //no locking required as this is only called when transform is single-threaded
        for (ResourceWrapper wrapper : allResources) {
            String type = wrapper.getResourceType();
            List<ResourceWrapper> l = hmByType.get(type);
            if (l == null) {
                l = new ArrayList<>();
                hmByType.put(type, l);
            }
            l.add(wrapper);
        }
        for (String type : hmByType.keySet()) {
            List<ResourceWrapper> l = hmByType.get(type);
            LOG.debug("Got " + type + " -> " + l.size());
        }
    }

    public List<ResourceWrapper> findResourcesForType(ResourceType resourceType) {

        String typeStr = resourceType.toString();
        List<ResourceWrapper> ret = new ArrayList<>();

        try {
            lock.lock();

            for (ResourceWrapper resource : this.allResources) {
                if (resource.getResourceType().equals(typeStr)) {
                    ret.add(resource);
                }
            }
        } finally {
            lock.unlock();
        }

        return ret;
    }


    public void setResourceAsSkipped(ResourceWrapper resourceWrapper) {
        String resourceReference = ReferenceHelper.createResourceReference(resourceWrapper.getResourceType(), resourceWrapper.getResourceIdStr());
        resourcesSkippedReferences.put(resourceReference, MAP_VAL);
    }

    public Patient getCachedPatient(Reference patientReference) throws Exception {

        //we should only ever come in here after populatePatientAndPersonIds() has been successfully called
        if (cachedPatient == null) {
            throw new Exception("Trying to get cached patient " + patientReference.getReference() + " but FHIR resource is null");
        }

        //all data in an exchange batch should be for the same patient, but validate this is the case
//        String patientId = ReferenceHelper.getReferenceId(patientReference);
//        if (!patientId.equals(cachedPatient.getId())) {
//            throw new Exception("Trying to get cached patient " + patientReference.getReference() + " but cached patient has ID " + patientId);
//        }

        return cachedPatient;
    }

    public Resource findOrRetrieveResource(Reference reference) throws Exception {

        ResourceWrapper ret = findOrRetrieveResourceWrapper(reference);
        if (ret == null
                || ret.isDeleted()) {
            return null;
        } else {
            return FhirResourceHelper.deserialiseResouce(ret);
        }
    }

    public ResourceWrapper findOrRetrieveResourceWrapper(Reference reference) throws Exception {

        //look in our resources map first
        try {
            lock.lock();

            String referenceStr = reference.getReference();
            ResourceWrapper ret = hmAllResourcesByReferenceString.get(referenceStr);
            if (ret != null) {
                if (ret.isDeleted()) {
                    return null;
                } else {
                    return ret;
                }
            }
        } finally {
            lock.unlock();
        }

        //if not in our map, then hit the DB
        ReferenceComponents comps = ReferenceHelper.getReferenceComponents(reference);
        ResourceDalI resourceDal = DalProvider.factoryResourceDal();

        return resourceDal.getCurrentVersion(getServiceId(), comps.getResourceType().toString(), UUID.fromString(comps.getId()));
    }

    public String getSnomedToBnfChapter(String snomedCodeString) throws Exception {

        String bnfReference = snomedToBnfChapter.get(snomedCodeString);

        if (bnfReference != null) {
            return bnfReference;
        }

        SnomedToBnfChapterDalI snomedToBnfChapterDal = DalProvider.factorySnomedToBnfChapter();
        String fullBnfChapterCodeString = snomedToBnfChapterDal.lookupSnomedCode(snomedCodeString);

        if (fullBnfChapterCodeString != null && fullBnfChapterCodeString.length() > 7) {
            bnfReference = fullBnfChapterCodeString.substring(0, 6);
            snomedToBnfChapter.put(snomedCodeString, bnfReference);
        }

        return bnfReference;

    }

    public boolean shouldClinicalConceptBeDeleted(CodeableConcept codeableConcept) throws Exception {
        return !isCodeableConceptSafe(codeableConcept);
    }

    /**
     * tests if a codeable concept is one that is known to break de-identification
     * because it contains something like a patient's name, address or DoB
     */
    public static boolean isCodeableConceptSafe(CodeableConcept codeableConcept) throws Exception {

        //9155. and 184099003 are "patient date of birth" which breaks de-identification
        if (codeableConcept.hasCoding()) {

            for (Coding coding : codeableConcept.getCoding()) {
                if (!coding.hasSystem()
                        || !coding.hasCode()) {
                    continue;
                }

                String system = coding.getSystem();
                String code = coding.getCode();

                if ((system.equals(FhirCodeUri.CODE_SYSTEM_READ2) && getProtectedCodesRead2().contains(code))
                        || (system.equals(FhirCodeUri.CODE_SYSTEM_CTV3) && getProtectedCodesCTV3().contains(code))
                        || (system.equals(FhirCodeUri.CODE_SYSTEM_SNOMED_CT) && getProtectedCodesSnomed().contains(code))) {

                    LOG.debug("Will not transform " + code + " in system " + system + " as breaks de-identification");
                    return false;
                }
            }
        }

        return true;
    }


    private static Set<String> getProtectedCodesSnomed() throws Exception {
        if (protectedCodesSnomed == null) {

            Map<String, String> map = ResourceParser.readCsvResourceIntoMap("SubscriberProtectedCodesSnomed.csv", "Code", "Term", CSVFormat.DEFAULT.withHeader());
            protectedCodesSnomed = map.keySet();
        }
        return protectedCodesSnomed;
    }

    private static Set<String> getProtectedCodesCTV3() throws Exception {
        if (protectedCodesCTV3 == null) {

            Map<String, String> map = ResourceParser.readCsvResourceIntoMap("SubscriberProtectedCodesCTV3.csv", "Code", "Term", CSVFormat.DEFAULT.withHeader());
            protectedCodesCTV3 = map.keySet();
        }
        return protectedCodesCTV3;
    }

    private static Set<String> getProtectedCodesRead2() throws Exception {
        if (protectedCodesRead2 == null) {

            Map<String, String> map = ResourceParser.readCsvResourceIntoMap("SubscriberProtectedCodesRead2.csv", "Code", "Term", CSVFormat.DEFAULT.withHeader());
            protectedCodesRead2 = map.keySet();
        }
        return protectedCodesRead2;
    }

    /**
     * finds an Organization FHIR reference that points to the same logical organisation as the service ID
     */
    public static Reference findOrganisationReferenceForPublisher(UUID serviceId) throws Exception {

        //the Patient Search table can give us a list of patient UUIDs, so just use that to find an ID
        //then retrieve that from the FHIR DB
        PatientSearchDalI patientSearchDal = DalProvider.factoryPatientSearchDal();
        List<UUID> patientIds = patientSearchDal.getPatientIds(serviceId, false, 1); //just get the first non-deleted one
        if (patientIds.isEmpty()) {
            //Emis sometimes activate practices before they send up patient data, so we may have a service with all the
            //non-patient metadata, but no patient data. If this happens, then don't send anything to Enterprise, as
            //it'll all be sorted out when they do send patient data.
            return null;
            //throw new TransformException("Cannot find a Patient resource for service " + serviceId + " and system " + systemId);
        }

        UUID patientId = patientIds.get(0);
        ResourceDalI resourceRepository = DalProvider.factoryResourceDal();
        Patient patient = (Patient)resourceRepository.getCurrentVersionAsResource(serviceId, ResourceType.Patient, patientId.toString());
        if (!patient.hasManagingOrganization()) {
            throw new TransformException("Patient " + patient.getId() + " doesn't have a managing org for service " + serviceId);
        }

        Reference orgReference = patient.getManagingOrganization();
        return orgReference;
    }

    public Date getDtLastTransformedPatient(UUID patientId) throws Exception {
        SubscriberPatientDateDalI dal = DalProvider.factorySubscriberDateDal();
        return dal.getDateLastTransformedPatient(getSubscriberConfigName(), patientId);
    }

    public void saveDtLastTransformedPatient() throws Exception {
        //if we didn't transform any patient, there's nothing to do
        if (this.patientTransformedWrapper == null) {
            return;
        }

        SubscriberPatientDateDalI dal = DalProvider.factorySubscriberDateDal();
        UUID patientUuid = patientTransformedWrapper.getResourceId();
        long patientId = getSubscriberPatientId().longValue();

        Date dtVersion = null;
        if (!patientTransformedWrapper.isDeleted()) {
            dtVersion = patientTransformedWrapper.getCreatedAt();
        }

        dal.saveDateLastTransformedPatient(getSubscriberConfigName(), patientUuid, patientId, dtVersion);
    }

    public void setDtLastTransformedPatient(ResourceWrapper resourceWrapper) {
        this.patientTransformedWrapper = resourceWrapper;
    }

    /*public static Reference findOrganisationReferenceForPublisher(UUID serviceId) throws Exception {

        ResourceDalI resourceRepository = DalProvider.factoryResourceDal();
        ResourceWrapper resourceByService = resourceRepository.getFirstResourceByService(serviceId, ResourceType.Patient);
        if (resourceByService == null) {
            //Emis sometimes activate practices before they send up patient data, so we may have a service with all the
            //non-patient metadata, but no patient data. If this happens, then don't send anything to Enterprise, as
            //it'll all be sorted out when they do send patient data.
            return null;
            //throw new TransformException("Cannot find a Patient resource for service " + serviceId + " and system " + systemId);
        }

        String json = resourceByService.getResourceData();
        //LOG.info("First resource for service " + serviceId + " is " + resourceByService.getResourceType() + " " + resourceByService.getResourceId());

        //if the first patient has been deleted, then we need to look at its history to find the JSON from when it wasn't deleted
        if (Strings.isNullOrEmpty(json)) {
            List<ResourceWrapper> history = resourceRepository.getResourceHistory(serviceId, resourceByService.getResourceType(), resourceByService.getResourceId());
            for (ResourceWrapper historyItem: history) {
                json = historyItem.getResourceData();
                if (!Strings.isNullOrEmpty(json)) {
                    break;
                }
            }
        }

        Patient patient = (Patient)FhirResourceHelper.deserialiseResouce(json);
        if (!patient.hasManagingOrganization()) {
            throw new TransformException("Patient " + patient.getId() + " doesn't have a managing org for service " + serviceId);
        }

        Reference orgReference = patient.getManagingOrganization();
        return orgReference;
    }*/


    public SubscriberConfig getConfig() {
        return config;
    }
}
