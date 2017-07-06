package org.endeavourhealth.transform.common.idmappers;

import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.hl7.fhir.instance.model.Practitioner;
import org.hl7.fhir.instance.model.Resource;

import java.util.Map;
import java.util.UUID;

public class IdMapperPractitioner extends BaseIdMapper {
    @Override
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
    public String getPatientId(Resource resource) throws PatientResourceException {
        throw new PatientResourceException(resource, true);
    }

    @Override
    public void remapIds(Resource resource, Map<String, String> idMappings) throws Exception {
        throw new Exception("Resource type not supported for remapping");
    }
}
