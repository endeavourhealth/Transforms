package org.endeavourhealth.transform.common.idmappers;

import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.hl7.fhir.instance.model.Organization;
import org.hl7.fhir.instance.model.Resource;

import java.util.Map;
import java.util.UUID;

public class IdMapperOrganization extends BaseIdMapper {
    @Override
    public boolean mapIds(Resource resource, UUID serviceId, UUID systemId, boolean mapResourceId) throws Exception {
        Organization organization = (Organization)resource;

        if (organization.hasIdentifier()) {
            super.mapIdentifiers(organization.getIdentifier(), serviceId, systemId);
        }
        if (organization.hasPartOf()) {
            super.mapReference(organization.getPartOf(), serviceId, systemId);
        }

        return super.mapCommonResourceFields(organization, serviceId, systemId, mapResourceId);
    }

    @Override
    public String getPatientId(Resource resource) throws PatientResourceException {
        throw new PatientResourceException(resource, true);
    }

    @Override
    public void remapIds(Resource resource, Map<String, String> idMappings) throws Exception {
        throw new Exception("Resource type not supported for remapping");
    }
}
