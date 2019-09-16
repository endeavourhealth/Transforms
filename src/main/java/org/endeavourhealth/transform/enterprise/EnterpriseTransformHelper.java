package org.endeavourhealth.transform.enterprise;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.ReferenceComponents;
import org.endeavourhealth.common.fhir.ReferenceHelper;
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
import org.endeavourhealth.transform.enterprise.outputModels.OutputContainer;
import org.endeavourhealth.transform.enterprise.transforms.AbstractEnterpriseTransformer;
import org.hl7.fhir.instance.model.Patient;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class EnterpriseTransformHelper implements HasServiceSystemAndExchangeIdI {
    private static final Logger LOG = LoggerFactory.getLogger(EnterpriseTransformHelper.class);

    private static final Object MAP_VAL = new Object(); //concurrent hash map requires non-null values, so just use this

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

    private int batchSize;
    private Long enterpriseOrganisationId = null;
    private Long enterprisePatientId = null;
    private Long enterprisePersonId = null;
    private Boolean shouldPatientRecordBeDeleted = null; //whether the record should exist in the enterprise DB (e.g. if confidential)
    private String exchangeBody = null; //nasty hack to give us a reference back to the original inbound raw exchange

    public EnterpriseTransformHelper(UUID serviceId, UUID systemId, UUID protocolId, UUID exchangeId, UUID batchId, String enterpriseConfigName,
                                     OutputContainer outputContainer, List<ResourceWrapper> allResources, String exchangeBody) {
        this.serviceId = serviceId;
        this.systemId = systemId;
        this.protocolId = protocolId;
        this.exchangeId = exchangeId;
        this.batchId = batchId;
        this.enterpriseConfigName = enterpriseConfigName;
        this.outputContainer = outputContainer;
        this.exchangeBody = exchangeBody;

        //hash the resources by reference to them, so the transforms can quickly look up dependant resources
        this.allResources = allResources;
        this.hmAllResourcesByReferenceString = hashResourcesByReference(allResources);

        //dumpResourceCountsByType();
    }

    private void dumpResourceCountsByType() {
        Map<String, List<ResourceWrapper>> hmByType = new HashMap<>();
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

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
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



    public Boolean getShouldPatientRecordBeDeleted() {
        if (shouldPatientRecordBeDeleted == null) {
            throw new RuntimeException("Null shouldPatientRecordBeDeleted variable - this should always have been set");
        }
        return shouldPatientRecordBeDeleted;
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
    private static Map<String, ResourceWrapper> hashResourcesByReference(List<ResourceWrapper> allResources) {

        Map<String, ResourceWrapper> ret = new HashMap<>();

        for (ResourceWrapper resource: allResources) {

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

        //see if this resource is already in our list of things to transform
        String reference = ReferenceHelper.createResourceReference(wrapper.getResourceType(), wrapper.getResourceIdStr());
        if (this.hmAllResourcesByReferenceString.containsKey(reference)) {
            return;
        }

        //if we passed the above check, then audit that the resource was added and add to the map
        ExchangeBatchExtraResourceDalI exchangeBatchExtraResourceDalI = DalProvider.factoryExchangeBatchExtraResourceDal(getEnterpriseConfigName());
        ResourceType type = wrapper.getResourceTypeObj();
        UUID resourceId = wrapper.getResourceId();
        exchangeBatchExtraResourceDalI.saveExtraResource(getExchangeId(), getBatchId(), type, resourceId);

        //and add the resource to be transformed
        this.allResources.add(wrapper);
        this.hmAllResourcesByReferenceString.put(reference, wrapper);

    }

    /**
     * all resources in a batch are for the same patient (or no patient at all), so rather than looking
     * up the Enterprise patient ID for each resource, we can do it once at the start. To do that
     * we need the Discovery patient ID from one of the resources.
     */
    private String findPatientIdFromResources() throws Exception {

        for (ResourceWrapper resourceWrapper: allResources) {

            ResourceType resourceType = resourceWrapper.getResourceTypeObj();
            if (!FhirResourceFiler.isPatientResource(resourceType)) {
                continue;
            }

            //if it's a patient resource, we can just check the
            if (resourceType == ResourceType.Patient) {
                return resourceWrapper.getResourceIdStr();
            }

            if (!resourceWrapper.isDeleted()) {
                try {
                    Resource resource = FhirResourceHelper.deserialiseResouce(resourceWrapper);
                    String patientId = IdHelper.getPatientId(resource);
                    if (Strings.isNullOrEmpty(patientId)) {
                        continue;
                    }

                    return patientId;

                } catch (PatientResourceException ex) {
                    //we've had this exception because a batch has ended up containing JUST
                    //a Slot resource, which means we can't get the patient ID. The matching Appointment
                    //resource was created in a separate exchange_batch, but errors meant this data was
                    //split into a separate batch. This being the case, the Slot will already have been sent
                    //to the subscriber, because that's manually done when the appointment is done. So we
                    //can safely ignore this
                    if (resourceType != ResourceType.Slot) {
                        throw ex;
                    }
                }
            }
        }

        return null;
    }

    public int getResourceCount() {
        return allResources.size();
    }

    public void populatePatientAndPersonIds() throws Exception {

        String discoveryPatientId = findPatientIdFromResources();
        if (Strings.isNullOrEmpty(discoveryPatientId)) {
            //if no patient ID the remaining resources are patient ones
            return;
        }

        Long enterprisePatientId = AbstractEnterpriseTransformer.findEnterpriseId(this, ResourceType.Patient.toString(), discoveryPatientId);
        if (enterprisePatientId == null) {
            //with the Homerton data, we just get data from a point in time, not historic data too, so we have some episodes of
            //care where we don't have patients. If we're in this situation, then don't send over the data.
            LOG.warn("No enterprise patient ID for patient " + discoveryPatientId + " so not doing patient resources");
            return;
            //throw new TransformException("No enterprise patient ID found for discovery patient " + discoveryPatientId);
        }

        this.enterprisePatientId = enterprisePatientId;

        PatientLinkDalI patientLinkDal = DalProvider.factoryPatientLinkDal();
        String discoveryPersonId = patientLinkDal.getPersonId(discoveryPatientId);
        if (Strings.isNullOrEmpty(discoveryPersonId)) {
            //if we've got some cases where we've got a deleted patient but non-deleted patient-related resources
            //all in the same batch, because Emis sent it like that. In that case we won't have a person ID, so
            //return out without processing any of the remaining resources, since they're for a deleted patient.
            return;
        }

        SubscriberPersonMappingDalI personMappingDal = DalProvider.factorySubscriberPersonMappingDal(getEnterpriseConfigName());
        this.enterprisePersonId = personMappingDal.findOrCreateEnterprisePersonId(discoveryPersonId);

        //retrieve the patient resource to see if the record has been deleted or is confidential
        Reference patientRef = ReferenceHelper.createReference(ResourceType.Patient, discoveryPatientId);
        ResourceWrapper wrapper = findOrRetrieveResource(patientRef);
        Patient patient = null;
        if (wrapper != null) {
            patient = (Patient) FhirSerializationHelper.deserializeResource(wrapper.getResourceData());
        }
        this.shouldPatientRecordBeDeleted = !shouldPatientBePresentInSubscriber(patient);
    }

    public static boolean shouldPatientBePresentInSubscriber(Patient patient) {

        //deleted records shouldn't be in subscriber DBs
        if (patient == null) {
            return false;
        }

        //confidential records shouldn't be in subscriber DBs
        if (AbstractEnterpriseTransformer.isConfidential(patient)) {
            return false;
        }

        //records without NHS numbers shouldn't be in subscriber DBs
//TODO - waiting for clarification on this
        /*String nhsNumber = IdentifierHelper.findNhsNumber(patient);
        if (Strings.isNullOrEmpty(nhsNumber)) {
            return false;
        }
*/
        return true;
    }

    public void checkForMissedResources() throws Exception {

        List<String> missedResources = new ArrayList<>();
        int countMissedResources = 0;

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

    public ResourceWrapper findResourceForReference(Reference reference) {
        String referenceStr = reference.getReference();
        return hmAllResourcesByReferenceString.get(referenceStr);
    }

    public List<ResourceWrapper> findResourcesForType(ResourceType resourceType) {

        String typeStr = resourceType.toString();
        List<ResourceWrapper> ret = new ArrayList<>();

        for (ResourceWrapper resource: this.allResources) {
            if (resource.getResourceType().equals(typeStr)) {
                ret.add(resource);
            }
        }

        return ret;
    }

    public void setResourceAsSkipped(ResourceWrapper resourceWrapper) {
        String resourceReference = ReferenceHelper.createResourceReference(resourceWrapper.getResourceType(), resourceWrapper.getResourceIdStr());
        resourcesSkippedReferences.put(resourceReference, MAP_VAL);
    }

    public ResourceWrapper findOrRetrieveResource(Reference reference) throws Exception {

        //look in our resources map first
        ResourceWrapper ret = findResourceForReference(reference);
        if (ret != null) {
            if (ret.isDeleted()) {
                return null;
            } else {
                return ret;
            }
        }

        //if not in our map, then hit the DB
        ReferenceComponents comps = ReferenceHelper.getReferenceComponents(reference);
        ResourceDalI resourceDal = DalProvider.factoryResourceDal();

        return resourceDal.getCurrentVersion(getServiceId(), comps.getResourceType().toString(), UUID.fromString(comps.getId()));
    }

}
