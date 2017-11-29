package org.endeavourhealth.transform.common.idmappers;

import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.Schedule;

import java.util.Map;
import java.util.Set;

public class IdMapperSchedule extends BaseIdMapper {

    @Override
    public void getResourceReferences(Resource resource, Set<String> referenceValues) throws Exception {
        Schedule schedule = (Schedule)resource;
        super.addCommonResourceReferences(schedule, referenceValues);

        if (schedule.hasIdentifier()) {
            super.addIndentifierReferences(schedule.getIdentifier(), referenceValues);
        }
        if (schedule.hasActor()) {
            super.addReference(schedule.getActor(), referenceValues);
        }
    }

    @Override
    public void applyReferenceMappings(Resource resource, Map<String, String> mappings, boolean failForMissingMappings) throws Exception {
        Schedule schedule = (Schedule)resource;
        super.mapCommonResourceFields(schedule, mappings, failForMissingMappings);

        if (schedule.hasIdentifier()) {
            super.mapIdentifiers(schedule.getIdentifier(), mappings, failForMissingMappings);
        }
        if (schedule.hasActor()) {
            super.mapReference(schedule.getActor(), mappings, failForMissingMappings);
        }
    }

    @Override
    public String getPatientId(Resource resource) throws PatientResourceException {
        throw new PatientResourceException(resource, true);
    }

    /*@Override
    public boolean mapIds(Resource resource, UUID serviceId, UUID systemId, boolean mapResourceId) throws Exception {
        Schedule schedule = (Schedule)resource;

        if (schedule.hasIdentifier()) {
            super.mapIdentifiers(schedule.getIdentifier(), serviceId, systemId);
        }
        if (schedule.hasActor()) {
            super.mapReference(schedule.getActor(), serviceId, systemId);
        }

        return super.mapCommonResourceFields(schedule, serviceId, systemId, mapResourceId);
    }


    @Override
    public void remapIds(Resource resource, Map<String, String> idMappings) throws Exception {
        throw new Exception("Resource type not supported for remapping");
    }*/
}
