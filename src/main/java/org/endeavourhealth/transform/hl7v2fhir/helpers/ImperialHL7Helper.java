package org.endeavourhealth.transform.hl7v2fhir.helpers;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.HasServiceSystemAndExchangeIdI;
import org.endeavourhealth.transform.common.IdHelper;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;


public class ImperialHL7Helper implements HasServiceSystemAndExchangeIdI {
    private static final Logger LOG = LoggerFactory.getLogger(ImperialHL7Helper.class);

    //DB access
    private ResourceDalI resourceRepository = DalProvider.factoryResourceDal();

    private final UUID serviceId;
    private final UUID systemId;
    private final UUID exchangeId;
    private final String dataSharingAgreementGuid;
    private final Map<Class, AbstractCsvParser> parsers;

    public ImperialHL7Helper(UUID serviceId, UUID systemId, UUID exchangeId, String dataSharingAgreementGuid, Map<Class, AbstractCsvParser> parsers) {
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

    /**
     *
     * @param locallyUniqueId
     * @param resourceType
     * @return
     * @throws Exception
     */
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
     *
     * @param resourceType
     * @param id
     * @return
     */
    public static String createResourceReference(String resourceType, String id) {
        return resourceType + "/" + id;
    }

    /**
     *
     * @param resourceType
     * @param id
     * @return
     */
    public static Reference createReference(ResourceType resourceType, String id) {
        return createReference(resourceType.toString(), id);
    }

    /**
     *
     * @param resourceType
     * @param id
     * @return
     */
    public static Reference createReference(String resourceType, String id) {
        if (StringUtils.isBlank(id)) {
            throw new IllegalArgumentException("Blank id when creating reference for " + resourceType);
        } else {
            return createReference(createResourceReference(resourceType, id));
        }
    }

    /**
     *
     * @param referenceValue
     * @return
     */
    public static Reference createReference(String referenceValue) {
        return (new Reference()).setReference(referenceValue);
    }

}

