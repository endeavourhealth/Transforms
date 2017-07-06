package org.endeavourhealth.transform.common.idmappers;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.hl7.fhir.instance.model.Procedure;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.Map;
import java.util.UUID;

public class IdMapperProcedure extends BaseIdMapper {
    @Override
    public boolean mapIds(Resource resource, UUID serviceId, UUID systemId, boolean mapResourceId) throws Exception {
        Procedure procedure = (Procedure)resource;

        if (procedure.hasIdentifier()) {
            super.mapIdentifiers(procedure.getIdentifier(), serviceId, systemId);
        }
        if (procedure.hasSubject()) {
            super.mapReference(procedure.getSubject(), serviceId, systemId);
        }
        if (procedure.hasPerformer()) {
            for (Procedure.ProcedurePerformerComponent performer: procedure.getPerformer()) {
                if (performer.hasActor()) {
                    super.mapReference(performer.getActor(), serviceId, systemId);
                }
            }
        }
        if (procedure.hasEncounter()) {
            super.mapReference(procedure.getEncounter(), serviceId, systemId);
        }
        if (procedure.hasLocation()) {
            super.mapReference(procedure.getLocation(), serviceId, systemId);
        }

        return super.mapCommonResourceFields(procedure, serviceId, systemId, mapResourceId);
    }

    @Override
    public String getPatientId(Resource resource) throws PatientResourceException {

        Procedure procedure = (Procedure)resource;
        if (procedure.hasSubject()) {
            return ReferenceHelper.getReferenceId(procedure.getSubject(), ResourceType.Patient);
        }
        return null;
    }

    @Override
    public void remapIds(Resource resource, Map<String, String> idMappings) throws Exception {
        throw new Exception("Resource type not supported for remapping");
    }
}
