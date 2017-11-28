package org.endeavourhealth.transform.common.idmappers;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.hl7.fhir.instance.model.ProcedureRequest;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.Map;
import java.util.Set;

public class IdMapperProcedureRequest extends BaseIdMapper {

    @Override
    public void getResourceReferences(Resource resource, Set<String> referenceValues) throws Exception {
        ProcedureRequest procedureRequest = (ProcedureRequest)resource;
        super.addCommonResourceReferences(procedureRequest, referenceValues);

        if (procedureRequest.hasIdentifier()) {
            super.addIndentifierReferences(procedureRequest.getIdentifier(), referenceValues);
        }
        if (procedureRequest.hasSubject()) {
            super.addReference(procedureRequest.getSubject(), referenceValues);
        }
        if (procedureRequest.hasReason()) {
            try {
                super.addReference(procedureRequest.getReasonReference(), referenceValues);
            } catch (Exception ex) {
                //do nothing if isn't a reference
            }
        }
        if (procedureRequest.hasEncounter()) {
            super.addReference(procedureRequest.getEncounter(), referenceValues);
        }
        if (procedureRequest.hasPerformer()) {
            super.addReference(procedureRequest.getPerformer(), referenceValues);
        }
        if (procedureRequest.hasOrderer()) {
            super.addReference(procedureRequest.getOrderer(), referenceValues);
        }
    }

    @Override
    public void applyReferenceMappings(Resource resource, Map<String, String> mappings) throws Exception {
        ProcedureRequest procedureRequest = (ProcedureRequest)resource;
        super.mapCommonResourceFields(procedureRequest, mappings);

        if (procedureRequest.hasIdentifier()) {
            super.mapIdentifiers(procedureRequest.getIdentifier(), mappings);
        }
        if (procedureRequest.hasSubject()) {
            super.mapReference(procedureRequest.getSubject(), mappings);
        }
        if (procedureRequest.hasReason()) {
            try {
                super.mapReference(procedureRequest.getReasonReference(), mappings);
            } catch (Exception ex) {
                //do nothing if isn't a reference
            }
        }
        if (procedureRequest.hasEncounter()) {
            super.mapReference(procedureRequest.getEncounter(), mappings);
        }
        if (procedureRequest.hasPerformer()) {
            super.mapReference(procedureRequest.getPerformer(), mappings);
        }
        if (procedureRequest.hasOrderer()) {
            super.mapReference(procedureRequest.getOrderer(), mappings);
        }
    }

    @Override
    public String getPatientId(Resource resource) throws PatientResourceException {

        ProcedureRequest procedureRequest = (ProcedureRequest)resource;
        if (procedureRequest.hasSubject()) {
            return ReferenceHelper.getReferenceId(procedureRequest.getSubject(), ResourceType.Patient);
        }
        return null;
    }

    /*@Override
    public boolean mapIds(Resource resource, UUID serviceId, UUID systemId, boolean mapResourceId) throws Exception {
        ProcedureRequest procedureRequest = (ProcedureRequest)resource;

        if (procedureRequest.hasIdentifier()) {
            super.mapIdentifiers(procedureRequest.getIdentifier(), serviceId, systemId);
        }
        if (procedureRequest.hasSubject()) {
            super.mapReference(procedureRequest.getSubject(), serviceId, systemId);
        }
        if (procedureRequest.hasReason()) {
            try {
                super.mapReference(procedureRequest.getReasonReference(), serviceId, systemId);
            } catch (Exception ex) {
                //do nothing if isn't a reference
            }
        }
        if (procedureRequest.hasEncounter()) {
            super.mapReference(procedureRequest.getEncounter(), serviceId, systemId);
        }
        if (procedureRequest.hasPerformer()) {
            super.mapReference(procedureRequest.getPerformer(), serviceId, systemId);
        }
        if (procedureRequest.hasOrderer()) {
            super.mapReference(procedureRequest.getOrderer(), serviceId, systemId);
        }

        return super.mapCommonResourceFields(procedureRequest, serviceId, systemId, mapResourceId);
    }

    @Override
    public void remapIds(Resource resource, Map<String, String> idMappings) throws Exception {
        throw new Exception("Resource type not supported for remapping");
    }*/
}
