package org.endeavourhealth.transform.common.idmappers;

import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.hl7.fhir.instance.model.Practitioner;
import org.hl7.fhir.instance.model.Resource;

import java.util.Map;
import java.util.Set;

public class IdMapperPractitioner extends BaseIdMapper {


    @Override
    public void getResourceReferences(Resource resource, Set<String> referenceValues) throws Exception {
        Practitioner practitioner = (Practitioner)resource;
        super.addCommonResourceReferences(practitioner, referenceValues);

        if (practitioner.hasIdentifier()) {
            super.addIndentifierReferences(practitioner.getIdentifier(), referenceValues);
        }
        if (practitioner.hasPractitionerRole()) {
            for (Practitioner.PractitionerPractitionerRoleComponent role: practitioner.getPractitionerRole()) {
                if (role.hasManagingOrganization()) {
                    super.addReference(role.getManagingOrganization(), referenceValues);
                }
                if (role.hasLocation()) {
                    super.addReferences(role.getLocation(), referenceValues);
                }
                if (role.hasHealthcareService()) {
                    super.addReferences(role.getHealthcareService(), referenceValues);
                }
            }
        }
    }

    @Override
    public void applyReferenceMappings(Resource resource, Map<String, String> mappings) throws Exception {
        Practitioner practitioner = (Practitioner)resource;
        super.mapCommonResourceFields(practitioner, mappings);

        if (practitioner.hasIdentifier()) {
            super.mapIdentifiers(practitioner.getIdentifier(), mappings);
        }
        if (practitioner.hasPractitionerRole()) {
            for (Practitioner.PractitionerPractitionerRoleComponent role: practitioner.getPractitionerRole()) {
                if (role.hasManagingOrganization()) {
                    super.mapReference(role.getManagingOrganization(), mappings);
                }
                if (role.hasLocation()) {
                    super.mapReferences(role.getLocation(), mappings);
                }
                if (role.hasHealthcareService()) {
                    super.mapReferences(role.getHealthcareService(), mappings);
                }
            }
        }
    }

    @Override
    public String getPatientId(Resource resource) throws PatientResourceException {
        throw new PatientResourceException(resource, true);
    }

    /*@Override
    public boolean mapIds(Resource resource, UUID serviceId, UUID systemId, boolean mapResourceId) throws Exception {
        Practitioner practitioner = (Practitioner)resource;

        if (practitioner.hasIdentifier()) {
            super.mapIdentifiers(practitioner.getIdentifier(), serviceId, systemId);
        }
        if (practitioner.hasPractitionerRole()) {
            for (Practitioner.PractitionerPractitionerRoleComponent role: practitioner.getPractitionerRole()) {
                if (role.hasManagingOrganization()) {
                    super.mapReference(role.getManagingOrganization(), serviceId, systemId);
                }
                if (role.hasLocation()) {
                    super.mapReferences(role.getLocation(), serviceId, systemId);
                }
                if (role.hasHealthcareService()) {
                    super.mapReferences(role.getHealthcareService(), serviceId, systemId);
                }
            }
        }

        return super.mapCommonResourceFields(practitioner, serviceId, systemId, mapResourceId);
    }

    @Override
    public void remapIds(Resource resource, Map<String, String> idMappings) throws Exception {
        throw new Exception("Resource type not supported for remapping");
    }*/
}
