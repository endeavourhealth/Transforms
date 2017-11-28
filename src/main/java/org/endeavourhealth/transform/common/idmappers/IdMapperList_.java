package org.endeavourhealth.transform.common.idmappers;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.hl7.fhir.instance.model.List_;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.Map;
import java.util.Set;

public class IdMapperList_ extends BaseIdMapper {



    @Override
    public void getResourceReferences(Resource resource, Set<String> referenceValues) throws Exception {
        List_ list = (List_)resource;
        super.addCommonResourceReferences(list, referenceValues);

        if (list.hasIdentifier()) {
            super.addIndentifierReferences(list.getIdentifier(), referenceValues);
        }
        if (list.hasSubject()) {
            super.addReference(list.getSubject(), referenceValues);
        }
        if (list.hasSource()) {
            super.addReference(list.getSource(), referenceValues);
        }
        if (list.hasEncounter()) {
            super.addReference(list.getEncounter(), referenceValues);
        }
        if (list.hasEntry()) {
            for (List_.ListEntryComponent entry: list.getEntry()) {
                if (entry.hasItem()) {
                    super.addReference(entry.getItem(), referenceValues);
                }
            }
        }
    }

    @Override
    public void applyReferenceMappings(Resource resource, Map<String, String> mappings) throws Exception {
        List_ list = (List_)resource;
        super.mapCommonResourceFields(list, mappings);

        if (list.hasIdentifier()) {
            super.mapIdentifiers(list.getIdentifier(), mappings);
        }
        if (list.hasSubject()) {
            super.mapReference(list.getSubject(), mappings);
        }
        if (list.hasSource()) {
            super.mapReference(list.getSource(), mappings);
        }
        if (list.hasEncounter()) {
            super.mapReference(list.getEncounter(), mappings);
        }
        if (list.hasEntry()) {
            for (List_.ListEntryComponent entry: list.getEntry()) {
                if (entry.hasItem()) {
                    super.mapReference(entry.getItem(), mappings);
                }
            }
        }
    }

    @Override
    public String getPatientId(Resource resource) throws PatientResourceException {

        List_ list = (List_)resource;
        if (list.hasSubject()) {
            return ReferenceHelper.getReferenceId(list.getSubject(), ResourceType.Patient);
        }
        return null;
    }

    /*@Override
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
    public void remapIds(Resource resource, Map<String, String> idMappings) throws Exception {
        throw new Exception("Resource type not supported for remapping");
    }*/
}
