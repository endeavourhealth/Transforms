package org.endeavourhealth.transform.enterprise;

import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.transform.enterprise.outputModels.OutputContainer;
import org.hl7.fhir.instance.model.Reference;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class EnterpriseTransformParams {

    private final UUID protocolId;
    private final UUID exchangeId;
    private final UUID batchId;
    private final String enterpriseConfigName;
    private final OutputContainer data;
    private final Map<String, ResourceWrapper> allResources;
    private final Set<String> resourcesTransformed;

    private int batchSize;
    private Long enterpriseOrganisationId = null;
    private Long enterprisePatientId = null;
    private Long enterprisePersonId = null;
    private String exchangeBody = null; //nasty hack to give us a reference back to the original inbound raw exchange

    public EnterpriseTransformParams(UUID protocolId, UUID exchangeId, UUID batchId, String enterpriseConfigName,
                                     OutputContainer data, Map<String, ResourceWrapper> allResources, String exchangeBody) {
        this.protocolId = protocolId;
        this.exchangeId = exchangeId;
        this.batchId = batchId;
        this.enterpriseConfigName = enterpriseConfigName;
        this.data = data;
        this.allResources = allResources;
        this.exchangeBody = exchangeBody;
        this.resourcesTransformed = new HashSet<>();
    }

    public String getExchangeBody() {
        return exchangeBody;
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

    public OutputContainer getData() {
        return data;
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
}
