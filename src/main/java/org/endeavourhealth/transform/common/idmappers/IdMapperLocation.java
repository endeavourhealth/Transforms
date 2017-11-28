package org.endeavourhealth.transform.common.idmappers;

import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.hl7.fhir.instance.model.Location;
import org.hl7.fhir.instance.model.Resource;

import java.util.Map;
import java.util.Set;

public class IdMapperLocation extends BaseIdMapper {

    @Override
    public void getResourceReferences(Resource resource, Set<String> referenceValues) throws Exception {
        Location location = (Location)resource;
        super.addCommonResourceReferences(location, referenceValues);

        if (location.hasIdentifier()) {
            super.addIndentifierReferences(location.getIdentifier(), referenceValues);
        }
        if (location.hasManagingOrganization()) {
            super.addReference(location.getManagingOrganization(), referenceValues);
        }
        if (location.hasPartOf()) {
            super.addReference(location.getPartOf(), referenceValues);
        }

    }

    @Override
    public void applyReferenceMappings(Resource resource, Map<String, String> mappings) throws Exception {
        Location location = (Location)resource;
        super.mapCommonResourceFields(location, mappings);

        if (location.hasIdentifier()) {
            super.mapIdentifiers(location.getIdentifier(), mappings);
        }
        if (location.hasManagingOrganization()) {
            super.mapReference(location.getManagingOrganization(), mappings);
        }
        if (location.hasPartOf()) {
            super.mapReference(location.getPartOf(), mappings);
        }

    }

    @Override
    public String getPatientId(Resource resource) throws PatientResourceException {
        throw new PatientResourceException(resource, true);
    }

    /*@Override
    public boolean mapIds(Resource resource, UUID serviceId, UUID systemId, boolean mapResourceId) throws Exception {
        Location location = (Location)resource;

        if (location.hasIdentifier()) {
            super.mapIdentifiers(location.getIdentifier(), serviceId, systemId);
        }
        if (location.hasManagingOrganization()) {
            super.mapReference(location.getManagingOrganization(), serviceId, systemId);
        }
        if (location.hasPartOf()) {
            super.mapReference(location.getPartOf(), serviceId, systemId);
        }

        return super.mapCommonResourceFields(location, serviceId, systemId, mapResourceId);
    }
    
    @Override
    public void remapIds(Resource resource, Map<String, String> idMappings) throws Exception {
        throw new Exception("Resource type not supported for remapping");
    }*/
}
