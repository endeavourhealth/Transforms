package org.endeavourhealth.transform.pcr.mocks;

import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.List;

public interface ResourceDAL {
    List<ResourceType> getResourceTypes();
    List<ResourceWrapper> getPatientResources(String serviceId, String systemId, String patientId, List<String> resourceTypes);
    Resource getResource(org.hl7.fhir.instance.model.ResourceType resourceType, String resourceId, String serviceId);
}