package org.endeavourhealth.transform.common.idmappers;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.hl7.fhir.instance.model.AllergyIntolerance;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.Map;
import java.util.Set;

public class IdMapperAllergyIntolerance extends BaseIdMapper {

    @Override
    public void getResourceReferences(Resource resource, Set<String> referenceValues) throws Exception {
        AllergyIntolerance allergyIntolerance = (AllergyIntolerance)resource;
        super.addCommonResourceReferences(allergyIntolerance, referenceValues);

        if (allergyIntolerance.hasIdentifier()) {
            super.addIndentifierReferences(allergyIntolerance.getIdentifier(), referenceValues);
        }
        if (allergyIntolerance.hasRecorder()) {
            super.addReference(allergyIntolerance.getRecorder(), referenceValues);
        }
        if (allergyIntolerance.hasPatient()) {
            super.addReference(allergyIntolerance.getPatient(), referenceValues);
        }
    }

    @Override
    public void applyReferenceMappings(Resource resource, Map<String, String> mappings) throws Exception {
        AllergyIntolerance allergyIntolerance = (AllergyIntolerance)resource;

        super.mapCommonResourceFields(allergyIntolerance, mappings);

        if (allergyIntolerance.hasIdentifier()) {
            super.mapIdentifiers(allergyIntolerance.getIdentifier(), mappings);
        }
        if (allergyIntolerance.hasRecorder()) {
            super.mapReference(allergyIntolerance.getRecorder(), mappings);
        }
        if (allergyIntolerance.hasPatient()) {
            super.mapReference(allergyIntolerance.getPatient(), mappings);
        }
    }

    @Override
    public String getPatientId(Resource resource) throws PatientResourceException {

        AllergyIntolerance allergyIntolerance = (AllergyIntolerance)resource;
        if (allergyIntolerance.hasPatient()) {
            return ReferenceHelper.getReferenceId(allergyIntolerance.getPatient(), ResourceType.Patient);
        }
        return null;
    }

    /*@Override
    public boolean mapIds(Resource resource, UUID serviceId, UUID systemId, boolean mapResourceId) throws Exception {
        AllergyIntolerance allergyIntolerance = (AllergyIntolerance)resource;

        if (allergyIntolerance.hasIdentifier()) {
            super.mapIdentifiers(allergyIntolerance.getIdentifier(), serviceId, systemId);
        }
        if (allergyIntolerance.hasRecorder()) {
            super.mapReference(allergyIntolerance.getRecorder(), serviceId, systemId);
        }
        if (allergyIntolerance.hasPatient()) {
            super.mapReference(allergyIntolerance.getPatient(), serviceId, systemId);
        }

        return super.mapCommonResourceFields(allergyIntolerance, serviceId, systemId, mapResourceId);
    }


    @Override
    public void remapIds(Resource resource, Map<String, String> idMappings) throws Exception {
        throw new Exception("Resource type not supported for remapping");
    }*/
}
