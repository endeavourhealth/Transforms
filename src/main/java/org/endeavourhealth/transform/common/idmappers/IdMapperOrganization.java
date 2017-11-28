package org.endeavourhealth.transform.common.idmappers;

import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.hl7.fhir.instance.model.Organization;
import org.hl7.fhir.instance.model.Resource;

import java.util.Map;
import java.util.Set;

public class IdMapperOrganization extends BaseIdMapper {


    @Override
    public void getResourceReferences(Resource resource, Set<String> referenceValues) throws Exception {
        Organization organization = (Organization)resource;
        super.addCommonResourceReferences(organization, referenceValues);

        if (organization.hasIdentifier()) {
            super.addIndentifierReferences(organization.getIdentifier(), referenceValues);
        }
        if (organization.hasPartOf()) {
            super.addReference(organization.getPartOf(), referenceValues);
        }
    }

    @Override
    public void applyReferenceMappings(Resource resource, Map<String, String> mappings) throws Exception {
        Organization organization = (Organization)resource;
        super.mapCommonResourceFields(organization, mappings);

        if (organization.hasIdentifier()) {
            super.mapIdentifiers(organization.getIdentifier(), mappings);
        }
        if (organization.hasPartOf()) {
            super.mapReference(organization.getPartOf(), mappings);
        }
    }

    @Override
    public String getPatientId(Resource resource) throws PatientResourceException {
        throw new PatientResourceException(resource, true);
    }

    /*@Override
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
    public void remapIds(Resource resource, Map<String, String> idMappings) throws Exception {
        throw new Exception("Resource type not supported for remapping");
    }*/
}
