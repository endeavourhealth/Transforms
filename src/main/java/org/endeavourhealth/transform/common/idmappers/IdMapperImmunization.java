package org.endeavourhealth.transform.common.idmappers;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.hl7.fhir.instance.model.Immunization;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.Map;
import java.util.Set;

public class IdMapperImmunization extends BaseIdMapper {


    @Override
    public void getResourceReferences(Resource resource, Set<String> referenceValues) throws Exception {
        Immunization immunization = (Immunization)resource;
        super.addCommonResourceReferences(immunization, referenceValues);

        if (immunization.hasIdentifier()) {
            super.addIndentifierReferences(immunization.getIdentifier(), referenceValues);
        }
        if (immunization.hasPatient()) {
            super.addReference(immunization.getPatient(), referenceValues);
        }
        if (immunization.hasPerformer()) {
            super.addReference(immunization.getPerformer(), referenceValues);
        }
        if (immunization.hasRequester()) {
            super.addReference(immunization.getRequester(), referenceValues);
        }
        if (immunization.hasEncounter()) {
            super.addReference(immunization.getEncounter(), referenceValues);
        }
        if (immunization.hasManufacturer()) {
            super.addReference(immunization.getManufacturer(), referenceValues);
        }
        if (immunization.hasLocation()) {
            super.addReference(immunization.getLocation(), referenceValues);
        }
        if (immunization.hasReaction()) {
            for (Immunization.ImmunizationReactionComponent reaction: immunization.getReaction()) {
                if (reaction.hasDetail()) {
                    super.addReference(reaction.getDetail(), referenceValues);
                }
            }
        }
        if (immunization.hasVaccinationProtocol()) {
            for (Immunization.ImmunizationVaccinationProtocolComponent protocol: immunization.getVaccinationProtocol()) {
                if (protocol.hasAuthority()) {
                    super.addReference(protocol.getAuthority(), referenceValues);
                }
            }
        }
    }

    @Override
    public void applyReferenceMappings(Resource resource, Map<String, String> mappings) throws Exception {
        Immunization immunization = (Immunization)resource;
        super.mapCommonResourceFields(immunization, mappings);

        if (immunization.hasIdentifier()) {
            super.mapIdentifiers(immunization.getIdentifier(), mappings);
        }
        if (immunization.hasPatient()) {
            super.mapReference(immunization.getPatient(), mappings);
        }
        if (immunization.hasPerformer()) {
            super.mapReference(immunization.getPerformer(), mappings);
        }
        if (immunization.hasRequester()) {
            super.mapReference(immunization.getRequester(), mappings);
        }
        if (immunization.hasEncounter()) {
            super.mapReference(immunization.getEncounter(), mappings);
        }
        if (immunization.hasManufacturer()) {
            super.mapReference(immunization.getManufacturer(), mappings);
        }
        if (immunization.hasLocation()) {
            super.mapReference(immunization.getLocation(), mappings);
        }
        if (immunization.hasReaction()) {
            for (Immunization.ImmunizationReactionComponent reaction: immunization.getReaction()) {
                if (reaction.hasDetail()) {
                    super.mapReference(reaction.getDetail(), mappings);
                }
            }
        }
        if (immunization.hasVaccinationProtocol()) {
            for (Immunization.ImmunizationVaccinationProtocolComponent protocol: immunization.getVaccinationProtocol()) {
                if (protocol.hasAuthority()) {
                    super.mapReference(protocol.getAuthority(), mappings);
                }
            }
        }
    }

    @Override
    public String getPatientId(Resource resource) throws PatientResourceException {

        Immunization immunization = (Immunization)resource;
        if (immunization.hasPatient()) {
            return ReferenceHelper.getReferenceId(immunization.getPatient(), ResourceType.Patient);
        }
        return null;
    }

    /*@Override
    public boolean mapIds(Resource resource, UUID serviceId, UUID systemId, boolean mapResourceId) throws Exception {
        Immunization immunization = (Immunization)resource;

        if (immunization.hasIdentifier()) {
            super.mapIdentifiers(immunization.getIdentifier(), serviceId, systemId);
        }
        if (immunization.hasPatient()) {
            super.mapReference(immunization.getPatient(), serviceId, systemId);
        }
        if (immunization.hasPerformer()) {
            super.mapReference(immunization.getPerformer(), serviceId, systemId);
        }
        if (immunization.hasRequester()) {
            super.mapReference(immunization.getRequester(), serviceId, systemId);
        }
        if (immunization.hasEncounter()) {
            super.mapReference(immunization.getEncounter(), serviceId, systemId);
        }
        if (immunization.hasManufacturer()) {
            super.mapReference(immunization.getManufacturer(), serviceId, systemId);
        }
        if (immunization.hasLocation()) {
            super.mapReference(immunization.getLocation(), serviceId, systemId);
        }
        if (immunization.hasReaction()) {
            for (Immunization.ImmunizationReactionComponent reaction: immunization.getReaction()) {
                if (reaction.hasDetail()) {
                    super.mapReference(reaction.getDetail(), serviceId, systemId);
                }
            }
        }
        if (immunization.hasVaccinationProtocol()) {
            for (Immunization.ImmunizationVaccinationProtocolComponent protocol: immunization.getVaccinationProtocol()) {
                if (protocol.hasAuthority()) {
                    super.mapReference(protocol.getAuthority(), serviceId, systemId);
                }
            }
        }

        return super.mapCommonResourceFields(immunization, serviceId, systemId, mapResourceId);
    }

    @Override
    public void remapIds(Resource resource, Map<String, String> idMappings) throws Exception {
        throw new Exception("Resource type not supported for remapping");
    }*/
}
