package org.endeavourhealth.transform.common.idmappers;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.hl7.fhir.instance.model.Procedure;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.Map;
import java.util.Set;

public class IdMapperProcedure extends BaseIdMapper {

    @Override
    public void getResourceReferences(Resource resource, Set<String> referenceValues) throws Exception {
        Procedure procedure = (Procedure)resource;
        super.addCommonResourceReferences(procedure, referenceValues);

        if (procedure.hasIdentifier()) {
            super.addIndentifierReferences(procedure.getIdentifier(), referenceValues);
        }
        if (procedure.hasSubject()) {
            super.addReference(procedure.getSubject(), referenceValues);
        }
        if (procedure.hasPerformer()) {
            for (Procedure.ProcedurePerformerComponent performer: procedure.getPerformer()) {
                if (performer.hasActor()) {
                    super.addReference(performer.getActor(), referenceValues);
                }
            }
        }
        if (procedure.hasEncounter()) {
            super.addReference(procedure.getEncounter(), referenceValues);
        }
        if (procedure.hasLocation()) {
            super.addReference(procedure.getLocation(), referenceValues);
        }
    }

    @Override
    public void applyReferenceMappings(Resource resource, Map<String, String> mappings) throws Exception {
        Procedure procedure = (Procedure)resource;
        super.mapCommonResourceFields(procedure, mappings);

        if (procedure.hasIdentifier()) {
            super.mapIdentifiers(procedure.getIdentifier(), mappings);
        }
        if (procedure.hasSubject()) {
            super.mapReference(procedure.getSubject(), mappings);
        }
        if (procedure.hasPerformer()) {
            for (Procedure.ProcedurePerformerComponent performer: procedure.getPerformer()) {
                if (performer.hasActor()) {
                    super.mapReference(performer.getActor(), mappings);
                }
            }
        }
        if (procedure.hasEncounter()) {
            super.mapReference(procedure.getEncounter(), mappings);
        }
        if (procedure.hasLocation()) {
            super.mapReference(procedure.getLocation(), mappings);
        }
    }

    @Override
    public String getPatientId(Resource resource) throws PatientResourceException {

        Procedure procedure = (Procedure)resource;
        if (procedure.hasSubject()) {
            return ReferenceHelper.getReferenceId(procedure.getSubject(), ResourceType.Patient);
        }
        return null;
    }

    /*@Override
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
    public void remapIds(Resource resource, Map<String, String> idMappings) throws Exception {
        throw new Exception("Resource type not supported for remapping");
    }*/
}
