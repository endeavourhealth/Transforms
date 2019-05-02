package org.endeavourhealth.transform.subscriber;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.transform.subscriber.outputModels.OutputContainer;
import org.hl7.fhir.instance.model.Reference;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class SubscriberTransformParams {

    private final UUID serviceId;
    private final UUID protocolId;
    private final UUID exchangeId;
    private final UUID batchId;
    private final String enterpriseConfigName;
    private final OutputContainer outputContainer;
    private final Map<String, ResourceWrapper> allResources;
    private final Set<String> resourcesTransformed;
    private final Map<String, SubscriberId> subscriberIdMap;

    private int batchSize;
    private Long enterpriseOrganisationId = null;
    private Long enterprisePatientId = null;
    private Long enterprisePersonId = null;
    private String exchangeBody = null; //nasty hack to give us a reference back to the original inbound raw exchange

    public SubscriberTransformParams(UUID serviceId, UUID protocolId, UUID exchangeId, UUID batchId, String enterpriseConfigName,
                                     OutputContainer outputContainer, Map<String, ResourceWrapper> allResources, String exchangeBody) {
        this.serviceId = serviceId;
        this.protocolId = protocolId;
        this.exchangeId = exchangeId;
        this.batchId = batchId;
        this.enterpriseConfigName = enterpriseConfigName;
        this.outputContainer = outputContainer;
        this.allResources = allResources;
        this.exchangeBody = exchangeBody;
        this.resourcesTransformed = new HashSet<>();
        this.subscriberIdMap = new ConcurrentHashMap<>();
    }



    public String getExchangeBody() {
        return exchangeBody;
    }

    public UUID getServiceId() {
        return serviceId;
    }

    public UUID getProtocolId() {
        return protocolId;
    }

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

    public Map<String, ResourceWrapper> getAllResources() {
        return allResources;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public Long getEnterpriseOrganisationId() {
        return enterpriseOrganisationId;
    }

    public void setEnterpriseOrganisationId(Long enterpriseOrganisationId) {
        if (this.enterpriseOrganisationId != null) {
            throw new IllegalArgumentException("Cannot change the enterpriseOrganisationId once set");
        }
        this.enterpriseOrganisationId = enterpriseOrganisationId;
    }

    public Long getEnterprisePatientId() {
        return enterprisePatientId;
    }

    public void setEnterprisePatientId(Long enterprisePatientId) {
        if (this.enterprisePatientId != null) {
            throw new IllegalArgumentException("Cannot change the enterprisePatientId once set");
        }
        this.enterprisePatientId = enterprisePatientId;
    }

    public Long getEnterprisePersonId() {
        return enterprisePersonId;
    }

    public void setEnterprisePersonId(Long enterprisePersonId) {
        if (this.enterprisePersonId != null) {
            throw new IllegalArgumentException("Cannot change the enterprisePersonId once set");
        }
        this.enterprisePersonId = enterprisePersonId;
    }

    public boolean hasResourceBeenTransformedAddIfNot(Reference reference) {
        //we have to use the Strings as the Reference class doesn't have hashCode or equals functions implmented
        String referenceVal = reference.getReference();
        boolean done = this.resourcesTransformed.contains(referenceVal);

        if (!done) {
            this.resourcesTransformed.add(referenceVal);
        }

        return done;
    }

    public void addAndUpdateSubscriberId(ResourceWrapper resourceWrapper, SubscriberId subscriberId) {
        if (subscriberId == null) {
            throw new RuntimeException("Can't cache null subscriber IDs");
        }

        //if we've just deleted the resource, set the datetime to null so that if the resource is un-deleted,
        //we know that it must be next sent as an insert rather than an update
        if (resourceWrapper.isDeleted()) {
            subscriberId.setDtUpdatedPreviouslySent(null);

        } else {
            subscriberId.setDtUpdatedPreviouslySent(resourceWrapper.getCreatedAt());
        }

        String reference = ReferenceHelper.createResourceReference(resourceWrapper.getResourceType(), resourceWrapper.getResourceId().toString());
        subscriberIdMap.put(reference, subscriberId);
    }

    public Map<String, SubscriberId> getSubscriberIdMap() {
        return subscriberIdMap;
    }
}
