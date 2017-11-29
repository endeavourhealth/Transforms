package org.endeavourhealth.transform.common.idmappers;

import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.Slot;

import java.util.Map;
import java.util.Set;

public class IdMapperSlot extends BaseIdMapper {


    @Override
    public void getResourceReferences(Resource resource, Set<String> referenceValues) throws Exception {
        Slot slot = (Slot)resource;
        super.addCommonResourceReferences(slot, referenceValues);

        if (slot.hasIdentifier()) {
            super.addIndentifierReferences(slot.getIdentifier(), referenceValues);
        }
        if (slot.hasSchedule()) {
            super.addReference(slot.getSchedule(), referenceValues);
        }
    }

    @Override
    public void applyReferenceMappings(Resource resource, Map<String, String> mappings, boolean failForMissingMappings) throws Exception {
        Slot slot = (Slot)resource;
        super.mapCommonResourceFields(slot, mappings, failForMissingMappings);

        if (slot.hasIdentifier()) {
            super.mapIdentifiers(slot.getIdentifier(), mappings, failForMissingMappings);
        }
        if (slot.hasSchedule()) {
            super.mapReference(slot.getSchedule(), mappings, failForMissingMappings);
        }
    }

    @Override
    public String getPatientId(Resource resource) throws PatientResourceException {
        throw new PatientResourceException(resource, true);
    }

    /*@Override
    public boolean mapIds(Resource resource, UUID serviceId, UUID systemId, boolean mapResourceId) throws Exception {
        Slot slot = (Slot)resource;

        if (slot.hasIdentifier()) {
            super.mapIdentifiers(slot.getIdentifier(), serviceId, systemId);
        }
        if (slot.hasSchedule()) {
            super.mapReference(slot.getSchedule(), serviceId, systemId);
        }

        return super.mapCommonResourceFields(slot, serviceId, systemId, mapResourceId);
    }

    @Override
    public void remapIds(Resource resource, Map<String, String> idMappings) throws Exception {
        throw new Exception("Resource type not supported for remapping");
    }*/
}
