package org.endeavourhealth.transform.common.idmappers;

import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.hl7.fhir.instance.model.Patient;
import org.hl7.fhir.instance.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

public class IdMapperPatient extends BaseIdMapper {
    private static final Logger LOG = LoggerFactory.getLogger(IdMapperPatient.class);

    @Override
    public void getResourceReferences(Resource resource, Set<String> referenceValues) throws Exception {
        Patient patient = (Patient)resource;
        super.addCommonResourceReferences(patient, referenceValues);

        if (patient.hasIdentifier()) {
            super.addIndentifierReferences(patient.getIdentifier(), referenceValues);
        }
        if (patient.hasContact()) {
            for (Patient.ContactComponent contact: patient.getContact()) {
                if (contact.hasOrganization()) {
                    super.addReference(contact.getOrganization(), referenceValues);
                }
            }
        }
        if (patient.hasCareProvider()) {
            super.addReferences(patient.getCareProvider(), referenceValues);
        }
        if (patient.hasManagingOrganization()) {
            super.addReference(patient.getManagingOrganization(), referenceValues);
        }
        if (patient.hasLink()) {
            for (Patient.PatientLinkComponent link: patient.getLink()) {
                if (link.hasOther()) {
                    super.addReference(link.getOther(), referenceValues);
                }
            }
        }
    }

    @Override
    public void applyReferenceMappings(Resource resource, Map<String, String> mappings, boolean failForMissingMappings) throws Exception {
        Patient patient = (Patient)resource;
        super.mapCommonResourceFields(patient, mappings, failForMissingMappings);

        if (patient.hasIdentifier()) {
            super.mapIdentifiers(patient.getIdentifier(), mappings, failForMissingMappings);
        }
        if (patient.hasContact()) {
            for (Patient.ContactComponent contact: patient.getContact()) {
                if (contact.hasOrganization()) {
                    super.mapReference(contact.getOrganization(), mappings, failForMissingMappings);
                }
            }
        }
        if (patient.hasCareProvider()) {
            super.mapReferences(patient.getCareProvider(), mappings, failForMissingMappings);
        }
        if (patient.hasManagingOrganization()) {
            LOG.info("Patient mapping: " + patient.getId() + "<>" + patient.getManagingOrganization().getId());
            super.mapReference(patient.getManagingOrganization(), mappings, failForMissingMappings);
        }
        if (patient.hasLink()) {
            for (Patient.PatientLinkComponent link: patient.getLink()) {
                if (link.hasOther()) {
                    super.mapReference(link.getOther(), mappings, failForMissingMappings);
                }
            }
        }
    }

    @Override
    public String getPatientId(Resource resource) throws PatientResourceException {

        Patient patient = (Patient)resource;
        return patient.getId();
    }

    /*@Override
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
    public void remapIds(Resource resource, Map<String, String> idMappings) throws Exception {
        throw new Exception("Resource type not supported for remapping");
    }*/
}
