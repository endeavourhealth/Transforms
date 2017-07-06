package org.endeavourhealth.transform.common.idmappers;

import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.hl7.fhir.instance.model.Patient;
import org.hl7.fhir.instance.model.Resource;

import java.util.Map;
import java.util.UUID;

public class IdMapperPatient extends BaseIdMapper {
    @Override
    public boolean mapIds(Resource resource, UUID serviceId, UUID systemId, boolean mapResourceId) throws Exception {
        Patient patient = (Patient)resource;

        if (patient.hasIdentifier()) {
            super.mapIdentifiers(patient.getIdentifier(), serviceId, systemId);
        }
        if (patient.hasContact()) {
            for (Patient.ContactComponent contact: patient.getContact()) {
                if (contact.hasOrganization()) {
                    super.mapReference(contact.getOrganization(), serviceId, systemId);
                }
            }
        }
        if (patient.hasCareProvider()) {
            super.mapReferences(patient.getCareProvider(), serviceId, systemId);
        }
        if (patient.hasManagingOrganization()) {
            super.mapReference(patient.getManagingOrganization(), serviceId, systemId);
        }
        if (patient.hasLink()) {
            for (Patient.PatientLinkComponent link: patient.getLink()) {
                if (link.hasOther()) {
                    super.mapReference(link.getOther(), serviceId, systemId);
                }
            }
        }

        return super.mapCommonResourceFields(patient, serviceId, systemId, mapResourceId);
    }

    @Override
    public String getPatientId(Resource resource) throws PatientResourceException {

        Patient patient = (Patient)resource;
        return patient.getId();
    }

    @Override
    public void remapIds(Resource resource, Map<String, String> idMappings) throws Exception {
        throw new Exception("Resource type not supported for remapping");
    }
}
