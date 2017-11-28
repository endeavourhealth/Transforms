package org.endeavourhealth.transform.common.idmappers;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.hl7.fhir.instance.model.Order;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.Map;
import java.util.Set;

public class IdMapperOrder extends BaseIdMapper {


    @Override
    public void getResourceReferences(Resource resource, Set<String> referenceValues) throws Exception {
        Order order = (Order)resource;
        super.addCommonResourceReferences(order, referenceValues);

        if (order.hasIdentifier()) {
            super.addIndentifierReferences(order.getIdentifier(), referenceValues);
        }
        if (order.hasSubject()) {
            super.addReference(order.getSubject(), referenceValues);
        }
        if (order.hasSource()) {
            super.addReference(order.getSource(), referenceValues);
        }
        if (order.hasTarget()) {
            super.addReference(order.getTarget(), referenceValues);
        }
        if (order.hasReason()) {
            try {
                super.addReference(order.getReasonReference(), referenceValues);
            } catch (Exception ex) {
                //not a problem if not a reference
            }
        }
        if (order.hasDetail()) {
            super.addReferences(order.getDetail(), referenceValues);
        }

    }

    @Override
    public void applyReferenceMappings(Resource resource, Map<String, String> mappings) throws Exception {
        Order order = (Order)resource;
        super.mapCommonResourceFields(order, mappings);

        if (order.hasIdentifier()) {
            super.mapIdentifiers(order.getIdentifier(), mappings);
        }
        if (order.hasSubject()) {
            super.mapReference(order.getSubject(), mappings);
        }
        if (order.hasSource()) {
            super.mapReference(order.getSource(), mappings);
        }
        if (order.hasTarget()) {
            super.mapReference(order.getTarget(), mappings);
        }
        if (order.hasReason()) {
            try {
                super.mapReference(order.getReasonReference(), mappings);
            } catch (Exception ex) {
                //not a problem if not a reference
            }
        }
        if (order.hasDetail()) {
            super.mapReferences(order.getDetail(), mappings);
        }

    }

    @Override
    public String getPatientId(Resource resource) throws PatientResourceException {

        Order order = (Order)resource;
        if (order.hasSubject()) {
            return ReferenceHelper.getReferenceId(order.getSubject(), ResourceType.Patient);
        }
        return null;
    }

    /*@Override
    public boolean mapIds(Resource resource, UUID serviceId, UUID systemId, boolean mapResourceId) throws Exception {
        Order order = (Order)resource;

        if (order.hasIdentifier()) {
            super.mapIdentifiers(order.getIdentifier(), serviceId, systemId);
        }
        if (order.hasSubject()) {
            super.mapReference(order.getSubject(), serviceId, systemId);
        }
        if (order.hasSource()) {
            super.mapReference(order.getSource(), serviceId, systemId);
        }
        if (order.hasTarget()) {
            super.mapReference(order.getTarget(), serviceId, systemId);
        }
        if (order.hasReason()) {
            try {
                super.mapReference(order.getReasonReference(), serviceId, systemId);
            } catch (Exception ex) {
                //not a problem if not a reference
            }
        }
        if (order.hasDetail()) {
            super.mapReferences(order.getDetail(), serviceId, systemId);
        }

        return super.mapCommonResourceFields(order, serviceId, systemId, mapResourceId);
    }

    @Override
    public void remapIds(Resource resource, Map<String, String> idMappings) throws Exception {
        throw new Exception("Resource type not supported for remapping");
    }*/
}
