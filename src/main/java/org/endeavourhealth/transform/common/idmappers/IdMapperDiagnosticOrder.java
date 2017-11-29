package org.endeavourhealth.transform.common.idmappers;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.hl7.fhir.instance.model.DiagnosticOrder;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.Map;
import java.util.Set;

public class IdMapperDiagnosticOrder extends BaseIdMapper {
    

    @Override
    public void getResourceReferences(Resource resource, Set<String> referenceValues) throws Exception {
        DiagnosticOrder order = (DiagnosticOrder)resource;
        super.addCommonResourceReferences(order, referenceValues);

        if (order.hasIdentifier()) {
            super.addIndentifierReferences(order.getIdentifier(), referenceValues);
        }
        if (order.hasSubject()) {
            super.addReference(order.getSubject(), referenceValues);
        }
        if (order.hasOrderer()) {
            super.addReference(order.getOrderer(), referenceValues);
        }
        if (order.hasEncounter()) {
            super.addReference(order.getEncounter(), referenceValues);
        }
        if (order.hasSupportingInformation()) {
            super.addReferences(order.getSupportingInformation(), referenceValues);
        }
        if (order.hasSpecimen()) {
            super.addReferences(order.getSpecimen(), referenceValues);
        }
        if (order.hasEvent()) {
            for (DiagnosticOrder.DiagnosticOrderEventComponent event: order.getEvent()) {
                if (event.hasActor()) {
                    super.addReference(event.getActor(), referenceValues);
                }
            }
        }
        if (order.hasItem()) {
            for (DiagnosticOrder.DiagnosticOrderItemComponent item: order.getItem()) {
                if (item.hasSpecimen()) {
                    super.addReferences(item.getSpecimen(), referenceValues);
                }
            }
        }
    }

    @Override
    public void applyReferenceMappings(Resource resource, Map<String, String> mappings, boolean failForMissingMappings) throws Exception {
        DiagnosticOrder order = (DiagnosticOrder)resource;

        super.mapCommonResourceFields(order, mappings, failForMissingMappings);

        if (order.hasIdentifier()) {
            super.mapIdentifiers(order.getIdentifier(), mappings, failForMissingMappings);
        }
        if (order.hasSubject()) {
            super.mapReference(order.getSubject(), mappings, failForMissingMappings);
        }
        if (order.hasOrderer()) {
            super.mapReference(order.getOrderer(), mappings, failForMissingMappings);
        }
        if (order.hasEncounter()) {
            super.mapReference(order.getEncounter(), mappings, failForMissingMappings);
        }
        if (order.hasSupportingInformation()) {
            super.mapReferences(order.getSupportingInformation(), mappings, failForMissingMappings);
        }
        if (order.hasSpecimen()) {
            super.mapReferences(order.getSpecimen(), mappings, failForMissingMappings);
        }
        if (order.hasEvent()) {
            for (DiagnosticOrder.DiagnosticOrderEventComponent event: order.getEvent()) {
                if (event.hasActor()) {
                    super.mapReference(event.getActor(), mappings, failForMissingMappings);
                }
            }
        }
        if (order.hasItem()) {
            for (DiagnosticOrder.DiagnosticOrderItemComponent item: order.getItem()) {
                if (item.hasSpecimen()) {
                    super.mapReferences(item.getSpecimen(), mappings, failForMissingMappings);
                }
            }
        }
    }

    @Override
    public String getPatientId(Resource resource) throws PatientResourceException {

        DiagnosticOrder order = (DiagnosticOrder)resource;
        if (order.hasSubject()) {
            return ReferenceHelper.getReferenceId(order.getSubject(), ResourceType.Patient);
        }
        return null;
    }

    /*@Override
    public boolean mapIds(Resource resource, UUID serviceId, UUID systemId, boolean mapResourceId) throws Exception {
        DiagnosticOrder order = (DiagnosticOrder)resource;

        if (order.hasIdentifier()) {
            super.mapIdentifiers(order.getIdentifier(), serviceId, systemId);
        }
        if (order.hasSubject()) {
            super.mapReference(order.getSubject(), serviceId, systemId);
        }
        if (order.hasOrderer()) {
            super.mapReference(order.getOrderer(), serviceId, systemId);
        }
        if (order.hasEncounter()) {
            super.mapReference(order.getEncounter(), serviceId, systemId);
        }
        if (order.hasSupportingInformation()) {
            super.mapReferences(order.getSupportingInformation(), serviceId, systemId);
        }
        if (order.hasSpecimen()) {
            super.mapReferences(order.getSpecimen(), serviceId, systemId);
        }
        if (order.hasEvent()) {
            for (DiagnosticOrder.DiagnosticOrderEventComponent event: order.getEvent()) {
                if (event.hasActor()) {
                    super.mapReference(event.getActor(), serviceId, systemId);
                }
            }
        }
        if (order.hasItem()) {
            for (DiagnosticOrder.DiagnosticOrderItemComponent item: order.getItem()) {
                if (item.hasSpecimen()) {
                    super.mapReferences(item.getSpecimen(), serviceId, systemId);
                }
            }
        }

        return super.mapCommonResourceFields(order, serviceId, systemId, mapResourceId);
    }
    
    @Override
    public void remapIds(Resource resource, Map<String, String> idMappings) throws Exception {
        throw new Exception("Resource type not supported for remapping");
    }*/
}
