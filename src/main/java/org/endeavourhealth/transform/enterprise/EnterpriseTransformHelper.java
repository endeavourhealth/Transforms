package org.endeavourhealth.transform.enterprise;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Strings;
import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.eds.PatientLinkDalI;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.subscriberTransform.ExchangeBatchExtraResourceDalI;
import org.endeavourhealth.core.database.dal.subscriberTransform.SubscriberPersonMappingDalI;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirResourceHelper;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.HasServiceSystemAndExchangeIdI;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.endeavourhealth.transform.enterprise.outputModels.AbstractEnterpriseCsvWriter;
import org.endeavourhealth.transform.enterprise.outputModels.OutputContainer;
import org.endeavourhealth.transform.enterprise.transforms.AbstractEnterpriseTransformer;
import org.endeavourhealth.transform.subscriber.SubscriberTransformHelper;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class EnterpriseTransformHelper implements HasServiceSystemAndExchangeIdI {
    private static final Logger LOG = LoggerFactory.getLogger(EnterpriseTransformHelper.class);

    private static final Object MAP_VAL = new Object(); //concurrent hash map requires non-null values, so just use this
    private static final int DEFAULT_TRANSFORM_BATCH_SIZE = 50;

    private final UUID serviceId;
    private final UUID systemId;
    private final UUID protocolId;
    private final UUID exchangeId;
    private final UUID batchId;
    private final String enterpriseConfigName;
    private final OutputContainer outputContainer;
    private final Map<String, ResourceWrapper> hmAllResourcesByReferenceString;
    private final List<ResourceWrapper> allResources;
    private final Map<String, Object> resourcesTransformedReferences = new ConcurrentHashMap<>(); //treated as a set, but need concurrent access
    private final Map<String, Object> resourcesSkippedReferences = new ConcurrentHashMap<>(); //treated as a set, but need concurrent access
    private final ReentrantLock lock = new ReentrantLock();
    private final boolean isPseudonymised;
    private final boolean includeDateRecorded;
    private final String excludeNhsNumberRegex;

    private int batchSize;
    private Long enterpriseOrganisationId = null;
    private Long enterprisePatientId = null;
    private Long enterprisePersonId = null;
    private Boolean shouldPatientRecordBeDeleted = null; //whether the record should exist in the enterprise DB (e.g. if confidential)
    private String exchangeBody = null; //nasty hack to give us a reference back to the original inbound raw exchange

    public EnterpriseTransformHelper(UUID serviceId, UUID systemId, UUID protocolId, UUID exchangeId, UUID batchId, String enterpriseConfigName,
                                     List<ResourceWrapper> allResources, String exchangeBody) throws Exception {
        this.serviceId = serviceId;
        this.systemId = systemId;
        this.protocolId = protocolId;
        this.exchangeId = exchangeId;
        this.batchId = batchId;
        this.enterpriseConfigName = enterpriseConfigName;
        this.exchangeBody = exchangeBody;

        //load our config record for some parameters
        JsonNode config = ConfigManager.getConfigurationAsJson(enterpriseConfigName, "db_subscriber");

        this.isPseudonymised = config.has("pseudonymisation");
        this.outputContainer = new OutputContainer(isPseudonymised);

        this.includeDateRecorded = config.has("include_date_recorded") && config.get("include_date_recorded").asBoolean();

        if (config.has("transform_batch_size")) {
            this.batchSize = config.get("transform_batch_size").asInt();
        } else {
            this.batchSize = DEFAULT_TRANSFORM_BATCH_SIZE;
        }

        if (config.has("excluded_nhs_number_regex")) {
            this.excludeNhsNumberRegex = config.get("excluded_nhs_number_regex").asText();
        } else {
            this.excludeNhsNumberRegex = null;
        }

        //hash the resources by reference to them, so the transforms can quickly look up dependant resources
        this.allResources = allResources;
        this.hmAllResourcesByReferenceString = hashResourcesByReference(allResources);

        //dumpResourceCountsByType();
    }

    private void dumpResourceCountsByType() {
        Map<String, List<ResourceWrapper>> hmByType = new HashMap<>();

        //no lock required because this only happens when the multi-threaded work is complete
        for (ResourceWrapper wrapper: allResources) {
            String type = wrapper.getResourceType();
            List<ResourceWrapper> l = hmByType.get(type);
            if (l == null) {
                l = new ArrayList<>();
                hmByType.put(type, l);
            }
            l.add(wrapper);
        }
        for (String type: hmByType.keySet()) {
            List<ResourceWrapper> l = hmByType.get(type);
            LOG.debug("Got " + type + " -> " + l.size());
        }
    }

    public String getExchangeBody() {
        return exchangeBody;
    }

    @Override
    public UUID getServiceId() {
        return serviceId;
    }

    @Override
    public UUID getSystemId() {
        return systemId;
    }

    public UUID getProtocolId() {
        return protocolId;
    }

    @Override
    public UUID getExchangeId() {
        return exchangeId;
    }

    public UUID getBatchId() {
        return batchId;
    }

    public String getEnterpriseConfigName() {
        return enterpriseConfigName;
    }

    public OutputContainer getOutputContainer() {
        return outputContainer;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public boolean isPseudonymised() {
        return isPseudonymised;
    }

    public Date includeDateRecorded(DomainResource fhir) {
        Date dateRecorded = null;
        if (includeDateRecorded) {
            Extension recordedDate = ExtensionConverter.findExtension(fhir, FhirExtensionUri.RECORDED_DATE);
            DateType value = (DateType) recordedDate.getValue();
            if (value.getValue() != null) {
                dateRecorded = value.getValue();
            }
        }
        return dateRecorded;
    }

    public Long getEnterpriseOrganisationId() {
        if (enterpriseOrganisationId == null) {
            throw new RuntimeException("Trying to get enterprise org ID but it's not been set");
        }
        return enterpriseOrganisationId;
    }

    public void setEnterpriseOrganisationId(Long enterpriseOrganisationId) {
        if (this.enterpriseOrganisationId != null) {
            throw new IllegalArgumentException("Cannot change the enterpriseOrganisationId once set");
        }
        this.enterpriseOrganisationId = enterpriseOrganisationId;
    }

    public Long getEnterprisePatientId() {
        if (enterprisePatientId == null) {
            throw new RuntimeException("Trying to get enterprise patient ID but it's not been set");
        }
        return enterprisePatientId;
    }


    public Long getEnterprisePersonId() {
        if (enterprisePersonId == null) {
            throw new RuntimeException("Trying to get enterprise person ID but it's not been set");
        }
        return enterprisePersonId;
    }



    public boolean getShouldPatientRecordBeDeleted() {
        if (shouldPatientRecordBeDeleted == null) {
            throw new RuntimeException("Null shouldPatientRecordBeDeleted variable - this should always have been set");
        }
        return shouldPatientRecordBeDeleted.booleanValue();
    }

    public boolean hasResourceBeenTransformedAddIfNot(ResourceWrapper resourceWrapper) {
        Reference resourceReference = ReferenceHelper.createReference(resourceWrapper.getResourceType(), resourceWrapper.getResourceIdStr());
        return hasResourceBeenTransformedAddIfNot(resourceReference);
    }

    public boolean hasResourceBeenTransformedAddIfNot(Reference reference) {
        //we have to use the Strings as the Reference class doesn't have hashCode or equals functions implmented
        String referenceVal = reference.getReference();
        boolean done = this.resourcesTransformedReferences.containsKey(referenceVal);

        if (!done) {
            this.resourcesTransformedReferences.put(referenceVal, MAP_VAL);
        }

        return done;
    }

    /**
     * hashes the resources by a reference to them, so the transforms can quickly look up dependant resources
     */
    private static Map<String, ResourceWrapper> hashResourcesByReference(List<ResourceWrapper> resourceWrappers) {

        Map<String, ResourceWrapper> ret = new HashMap<>();

        for (ResourceWrapper resource: resourceWrappers) {

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


    public void addResourceToTransform(ResourceWrapper wrapper) throws Exception {

        String reference = ReferenceHelper.createResourceReference(wrapper.getResourceType(), wrapper.getResourceIdStr());

        try {
            //need to lock because we may be trying to iterate through the list in another thread
            lock.lock();

            //see if this resource is already in our list of things to transform
            if (this.hmAllResourcesByReferenceString.containsKey(reference)) {
                return;
            }

        } finally {
            lock.unlock();
        }

        //if we passed the above check, then audit that the resource was added and add to the map
        ExchangeBatchExtraResourceDalI exchangeBatchExtraResourceDalI = DalProvider.factoryExchangeBatchExtraResourceDal(getEnterpriseConfigName());
        ResourceType type = wrapper.getResourceTypeObj();
        UUID resourceId = wrapper.getResourceId();

        //when invoked by the Bulk UPRN utility we don't have an exchange ID, so skip this in that case
        if (getExchangeId() != null) {
            exchangeBatchExtraResourceDalI.saveExtraResource(getExchangeId(), getBatchId(), type, resourceId);
        }

        //and add the resource to be transformed
        try {
            //need to lock because we may be trying to iterate through the list in another thread
            lock.lock();

            this.allResources.add(wrapper);
            this.hmAllResourcesByReferenceString.put(reference, wrapper);
        } finally {
            lock.unlock();
        }
    }

    /**
     * all resources in a batch are for the same patient (or no patient at all), so rather than looking
     * up the Enterprise patient ID for each resource, we can do it once at the start. To do that
     * we need the Discovery patient ID from one of the resources.
     */
    private String findPatientIdFromResources() throws Exception {

        //no locking required as this is only called from a single-threaded bit of the transform
        for (ResourceWrapper resourceWrapper: allResources) {

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

    public int getResourceCount() {
        //no locking required as this is only called from a single-threaded bit of the transform
        return allResources.size();
    }

    public void populatePatientAndPersonIds() throws Exception {

        String discoveryPatientId = findPatientIdFromResources();
        if (Strings.isNullOrEmpty(discoveryPatientId)) {
            //if no patient ID the remaining resources are patient ones
            return;
        }

        //retrieve the patient resource to see if the record has been deleted or is confidential
        Reference patientRef = ReferenceHelper.createReference(ResourceType.Patient, discoveryPatientId);
        ResourceWrapper patientWrapper = findOrRetrieveResource(patientRef);
        Patient patient = null;
        if (patientWrapper != null) {
            patient = (Patient) FhirSerializationHelper.deserializeResource(patientWrapper.getResourceData());
        }
        this.shouldPatientRecordBeDeleted = !shouldPatientBePresentInSubscriber(patient);


        PatientLinkDalI patientLinkDal = DalProvider.factoryPatientLinkDal();
        String discoveryPersonId = patientLinkDal.getPersonId(discoveryPatientId);
        if (Strings.isNullOrEmpty(discoveryPersonId)) {
            //if we've got some cases where we've got a deleted patient but non-deleted patient-related resources
            //all in the same batch, because Emis sent it like that. In that case we won't have a person ID, so
            //return out without processing any of the remaining resources, since they're for a deleted patient.
            this.shouldPatientRecordBeDeleted = new Boolean(true);
        } else {
            SubscriberPersonMappingDalI personMappingDal = DalProvider.factorySubscriberPersonMappingDal(getEnterpriseConfigName());
            this.enterprisePersonId = personMappingDal.findOrCreateEnterprisePersonId(discoveryPersonId);
        }


        this.enterprisePatientId = AbstractEnterpriseTransformer.findEnterpriseId(this, ResourceType.Patient.toString(), discoveryPatientId);
        if (enterprisePatientId == null) {
            //if we have no patient ID yet, then it's because our Patient resource has never been through this transform,
            //possibly because the patient has just entered the protocol cohort. The protocol Queue Reader
            //should detect that in the future, but for now, we need to simply force through the transform for the other resources now
            UUID patientUuid = UUID.fromString(discoveryPatientId);
            ResourceDalI resourceDal = DalProvider.factoryResourceDal();
            List<ResourceWrapper> allPatientResources = resourceDal.getResourcesByPatient(serviceId, patientUuid);
            for (ResourceWrapper wrapper: allPatientResources) {

                //add the resource so it gets transformed
                addResourceToTransform(wrapper);

                //if the patient record, we need to manually invoke the transform because this fn is only
                //called AFTER the patient has been transformed
                if (wrapper.getResourceTypeObj() == ResourceType.Patient) {
                    AbstractEnterpriseTransformer transformer = FhirToEnterpriseCsvTransformer.createTransformerForResourceType(ResourceType.Patient);
                    List<ResourceWrapper> l = new ArrayList<>();
                    l.add(wrapper);
                    AbstractEnterpriseCsvWriter writer = outputContainer.getPatients();
                    transformer.transformResources(l, writer, this);
                }
            }

            //now look for the enterprise patient ID again, if still null it means our patient was deleted (or never was received),
            //so set the boolean to delete everything from our subscriber DB
            this.enterprisePatientId = AbstractEnterpriseTransformer.findEnterpriseId(this, ResourceType.Patient.toString(), discoveryPatientId);
            if (enterprisePatientId == null) {
                LOG.warn("No enterprise patient ID for patient " + discoveryPatientId + " so will delete everything");
                this.shouldPatientRecordBeDeleted = new Boolean(true);
            }
        }
    }


    public boolean shouldPatientBePresentInSubscriber(Patient patient) {
        return SubscriberTransformHelper.shouldPatientBePresentInSubscriber(patient, this.excludeNhsNumberRegex);
    }


    public void checkForMissedResources() throws Exception {

        List<String> missedResources = new ArrayList<>();
        int countMissedResources = 0;

        //no locking required as this is only called from a single-threaded bit of the transform
        for (String referenceStr: hmAllResourcesByReferenceString.keySet()) {
            if (!resourcesTransformedReferences.containsKey(referenceStr)
                    && !resourcesSkippedReferences.containsKey(referenceStr)) {

                countMissedResources ++;
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


    public List<ResourceWrapper> findResourcesForType(ResourceType resourceType) {

        String typeStr = resourceType.toString();
        List<ResourceWrapper> ret = new ArrayList<>();

        try {
            lock.lock(); //need to lock as another thread may be trying to add resources

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

    public ResourceWrapper findOrRetrieveResource(Reference reference) throws Exception {

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

    public boolean shouldClinicalConceptBeDeleted(CodeableConcept codeableConcept) throws Exception {
        return !SubscriberTransformHelper.isCodeableConceptSafe(codeableConcept);
    }
}
