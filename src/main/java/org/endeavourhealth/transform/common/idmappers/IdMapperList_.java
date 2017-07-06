package org.endeavourhealth.transform.common.idmappers;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.hl7.fhir.instance.model.List_;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.Map;
import java.util.UUID;

public class IdMapperList_ extends BaseIdMapper {

    @Override
    public boolean mapIds(Resource resource, UUID serviceId, UUID systemId, boolean mapResourceId) throws Exception {
        List_ list = (List_)resource;

        if (list.hasIdentifier()) {
            super.mapIdentifiers(list.getIdentifier(), serviceId, systemId);
        }
        if (list.hasSubject()) {
            super.mapReference(list.getSubject(), serviceId, systemId);
        }
        if (list.hasSource()) {
            super.mapReference(list.getSource(), serviceId, systemId);
        }
        if (list.hasEncounter()) {
            super.mapReference(list.getEncounter(), serviceId, systemId);
        }
        if (list.hasEntry()) {
            for (List_.ListEntryComponent entry: list.getEntry()) {
                if (entry.hasItem()) {
                    super.mapReference(entry.getItem(), serviceId, systemId);
                }
            }
        }

        return super.mapCommonResourceFields(list, serviceId, systemId, mapResourceId);
    }

    @Override
    public String getPatientId(Resource resource) throws PatientResourceException {

        List_ list = (List_)resource;
        if (list.hasSubject()) {
            return ReferenceHelper.getReferenceId(list.getSubject(), ResourceType.Patient);
        }
        return null;
    }

    @Override
    public void remapIds(Resource resource, Map<String, String> idMappings) throws Exception {
        throw new Exception("Resource type not supported for remapping");
    }
}
