package org.endeavourhealth.transform.common.idmappers;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.hl7.fhir.instance.model.Observation;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.Map;
import java.util.Set;

public class IdMapperObservation extends BaseIdMapper {


    @Override
    public void getResourceReferences(Resource resource, Set<String> referenceValues) throws Exception {
        Observation observation = (Observation)resource;
        super.addCommonResourceReferences(observation, referenceValues);

        if (observation.hasIdentifier()) {
            super.addIndentifierReferences(observation.getIdentifier(), referenceValues);
        }
        if (observation.hasSubject()) {
            super.addReference(observation.getSubject(), referenceValues);
        }
        if (observation.hasEncounter()) {
            super.addReference(observation.getEncounter(), referenceValues);
        }
        if (observation.hasPerformer()) {
            super.addReferences(observation.getPerformer(), referenceValues);
        }
        if (observation.hasSpecimen()) {
            super.addReference(observation.getSpecimen(), referenceValues);
        }
        if (observation.hasDevice()) {
            super.addReference(observation.getDevice(), referenceValues);
        }
        if (observation.hasRelated()) {
            for (Observation.ObservationRelatedComponent related: observation.getRelated()) {
                if (related.hasTarget()) {
                    super.addReference(related.getTarget(), referenceValues);
                }
            }
        }
    }

    @Override
    public void applyReferenceMappings(Resource resource, Map<String, String> mappings, boolean failForMissingMappings) throws Exception {
        Observation observation = (Observation)resource;
        super.mapCommonResourceFields(observation, mappings, failForMissingMappings);

        if (observation.hasIdentifier()) {
            super.mapIdentifiers(observation.getIdentifier(), mappings, failForMissingMappings);
        }
        if (observation.hasSubject()) {
            super.mapReference(observation.getSubject(), mappings, failForMissingMappings);
        }
        if (observation.hasEncounter()) {
            super.mapReference(observation.getEncounter(), mappings, failForMissingMappings);
        }
        if (observation.hasPerformer()) {
            super.mapReferences(observation.getPerformer(), mappings, failForMissingMappings);
        }
        if (observation.hasSpecimen()) {
            super.mapReference(observation.getSpecimen(), mappings, failForMissingMappings);
        }
        if (observation.hasDevice()) {
            super.mapReference(observation.getDevice(), mappings, failForMissingMappings);
        }
        if (observation.hasRelated()) {
            for (Observation.ObservationRelatedComponent related: observation.getRelated()) {
                if (related.hasTarget()) {
                    super.mapReference(related.getTarget(), mappings, failForMissingMappings);
                }
            }
        }
    }

    @Override
    public String getPatientId(Resource resource) throws PatientResourceException {

        Observation observation = (Observation)resource;
        if (observation.hasSubject()) {
            return ReferenceHelper.getReferenceId(observation.getSubject(), ResourceType.Patient);
        }
        return null;
    }

    /*@Override
    public boolean mapIds(Resource resource, UUID serviceId, UUID systemId, boolean mapResourceId) throws Exception {
        Observation observation = (Observation)resource;

        if (observation.hasIdentifier()) {
            super.mapIdentifiers(observation.getIdentifier(), serviceId, systemId);
        }
        if (observation.hasSubject()) {
            super.mapReference(observation.getSubject(), serviceId, systemId);
        }
        if (observation.hasEncounter()) {
            super.mapReference(observation.getEncounter(), serviceId, systemId);
        }
        if (observation.hasPerformer()) {
            super.mapReferences(observation.getPerformer(), serviceId, systemId);
        }
        if (observation.hasSpecimen()) {
            super.mapReference(observation.getSpecimen(), serviceId, systemId);
        }
        if (observation.hasDevice()) {
            super.mapReference(observation.getDevice(), serviceId, systemId);
        }
        if (observation.hasRelated()) {
            for (Observation.ObservationRelatedComponent related: observation.getRelated()) {
                if (related.hasTarget()) {
                    super.mapReference(related.getTarget(), serviceId, systemId);
                }
            }
        }

        return super.mapCommonResourceFields(observation, serviceId, systemId, mapResourceId);
    }

    @Override
    public void remapIds(Resource resource, Map<String, String> idMappings) throws Exception {
        throw new Exception("Resource type not supported for remapping");
    }*/
}
