package org.endeavourhealth.transform.common.idmappers;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.hl7.fhir.instance.model.Condition;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.Map;
import java.util.Set;

public class IdMapperCondition extends BaseIdMapper {


    @Override
    public void getResourceReferences(Resource resource, Set<String> referenceValues) throws Exception {
        Condition condition = (Condition)resource;
        super.addCommonResourceReferences(condition, referenceValues);

        if (condition.hasIdentifier()) {
            super.addIndentifierReferences(condition.getIdentifier(), referenceValues);
        }
        if (condition.hasPatient()) {
            super.addReference(condition.getPatient(), referenceValues);
        }
        if (condition.hasEncounter()) {
            super.addReference(condition.getEncounter(), referenceValues);
        }
        if (condition.hasAsserter()) {
            super.addReference(condition.getAsserter(), referenceValues);
        }
    }

    @Override
    public void applyReferenceMappings(Resource resource, Map<String, String> mappings, boolean failForMissingMappings) throws Exception {
        Condition condition = (Condition)resource;
        super.mapCommonResourceFields(condition, mappings, failForMissingMappings);

        if (condition.hasIdentifier()) {
            super.mapIdentifiers(condition.getIdentifier(), mappings, failForMissingMappings);
        }
        if (condition.hasPatient()) {
            super.mapReference(condition.getPatient(), mappings, failForMissingMappings);
        }
        if (condition.hasEncounter()) {
            super.mapReference(condition.getEncounter(), mappings, failForMissingMappings);
        }
        if (condition.hasAsserter()) {
            super.mapReference(condition.getAsserter(), mappings, failForMissingMappings);
        }
    }

    @Override
    public String getPatientId(Resource resource) throws PatientResourceException {

        Condition condition = (Condition)resource;
        if (condition.hasPatient()) {
            return ReferenceHelper.getReferenceId(condition.getPatient(), ResourceType.Patient);
        }
        return null;
    }

    /*@Override
    public boolean mapIds(Resource resource, UUID serviceId, UUID systemId, boolean mapResourceId) throws Exception {
        Condition condition = (Condition)resource;

        if (condition.hasIdentifier()) {
            super.mapIdentifiers(condition.getIdentifier(), serviceId, systemId);
        }
        if (condition.hasPatient()) {
            super.mapReference(condition.getPatient(), serviceId, systemId);
        }
        if (condition.hasEncounter()) {
            super.mapReference(condition.getEncounter(), serviceId, systemId);
        }
        if (condition.hasAsserter()) {
            super.mapReference(condition.getAsserter(), serviceId, systemId);
        }

        return super.mapCommonResourceFields(condition, serviceId, systemId, mapResourceId);
    }

    @Override
    public void remapIds(Resource resource, Map<String, String> idMappings) throws Exception {
        throw new Exception("Resource type not supported for remapping");
    }*/
}
