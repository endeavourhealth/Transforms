package org.endeavourhealth.transform.pcr;

import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.transform.pcr.outputModels.OutputContainer;
import org.hl7.fhir.instance.model.Reference;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class PcrTransformParams {

    private final UUID serviceId;
    private final UUID systemId;
    private final UUID protocolId;
    private final UUID exchangeId;
    private final UUID batchId;
    private final String configName;
    private final OutputContainer outputContainer;
    private final Map<String, ResourceWrapper> allResources;
    private final Set<String> resourcesTransformed;
    private final boolean useInstanceMapping;

    private int batchSize;
    private Long pcrOrganisationId = null;
    private Long pcrPatientId = null;
    private String exchangeBody = null; //nasty hack to give us a reference back to the original inbound raw exchange

    public PcrTransformParams(UUID serviceId, UUID systemId, UUID protocolId, UUID exchangeId, UUID batchId, String configName,
                              OutputContainer outputContainer, Map<String, ResourceWrapper> allResources, String exchangeBody,
                              boolean useInstanceMapping) {
        this.serviceId = serviceId;
        this.systemId = systemId;
        this.protocolId = protocolId;
        this.exchangeId = exchangeId;
        this.batchId = batchId;
        this.configName = configName;
        this.outputContainer = outputContainer;
        this.allResources = allResources;
        this.exchangeBody = exchangeBody;
        this.resourcesTransformed = new HashSet<>();
        this.useInstanceMapping = useInstanceMapping;
    }

    public boolean isUseInstanceMapping() {
        return useInstanceMapping;
    }

    public String getExchangeBody() {
        return exchangeBody;
    }

    public UUID getServiceId() {
        return serviceId;
    }

    public UUID getSystemId() { return  serviceId; }

    public UUID getProtocolId() {
        return protocolId;
    }

    public UUID getExchangeId() {
        return exchangeId;
    }

    public UUID getBatchId() {
        return batchId;
    }

    public String getConfigName() {
        return configName;
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

    public Long getPcrOrganisationId() {
        return pcrOrganisationId;
    }

    public void setPcrOrganisationId(Long pcrOrganisationId) {
        if (this.pcrOrganisationId != null) {
            throw new IllegalArgumentException("Cannot change the pcrOrganisationId once set");
        }
        this.pcrOrganisationId = pcrOrganisationId;
    }

    public Long getPcrPatientId() {
        return pcrPatientId;
    }

    public void setPcrPatientId(Long pcrPatientId) {
        if (this.pcrPatientId != null) {
            throw new IllegalArgumentException("Cannot change the setPcrPatientId once set");
        }
        this.pcrPatientId = pcrPatientId;
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
