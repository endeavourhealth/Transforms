package org.endeavourhealth.transform.common.idmappers;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.hl7.fhir.instance.model.RelatedPerson;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.Map;
import java.util.Set;

public class IdMapperRelatedPerson extends BaseIdMapper {


    @Override
    public void getResourceReferences(Resource resource, Set<String> referenceValues) throws Exception {
        RelatedPerson relatedPerson = (RelatedPerson)resource;
        super.addCommonResourceReferences(relatedPerson, referenceValues);

        if (relatedPerson.hasIdentifier()) {
            super.addIndentifierReferences(relatedPerson.getIdentifier(), referenceValues);
        }
        if (relatedPerson.hasPatient()) {
            super.addReference(relatedPerson.getPatient(), referenceValues);
        }
    }

    @Override
    public void applyReferenceMappings(Resource resource, Map<String, String> mappings, boolean failForMissingMappings) throws Exception {
        RelatedPerson relatedPerson = (RelatedPerson)resource;
        super.mapCommonResourceFields(relatedPerson, mappings, failForMissingMappings);

        if (relatedPerson.hasIdentifier()) {
            super.mapIdentifiers(relatedPerson.getIdentifier(), mappings, failForMissingMappings);
        }
        if (relatedPerson.hasPatient()) {
            super.mapReference(relatedPerson.getPatient(), mappings, failForMissingMappings);
        }
    }

    @Override
    public String getPatientId(Resource resource) throws PatientResourceException {

        RelatedPerson relatedPerson = (RelatedPerson)resource;
        if (relatedPerson.hasPatient()) {
            return ReferenceHelper.getReferenceId(relatedPerson.getPatient(), ResourceType.Patient);
        }
        return null;
    }

    /*@Override
    public boolean mapIds(Resource resource, UUID serviceId, UUID systemId, boolean mapResourceId) throws Exception {
        RelatedPerson relatedPerson = (RelatedPerson)resource;

        if (relatedPerson.hasIdentifier()) {
            super.mapIdentifiers(relatedPerson.getIdentifier(), serviceId, systemId);
        }
        if (relatedPerson.hasPatient()) {
            super.mapReference(relatedPerson.getPatient(), serviceId, systemId);
        }

        return super.mapCommonResourceFields(relatedPerson, serviceId, systemId, mapResourceId);
    }

    @Override
    public void remapIds(Resource resource, Map<String, String> idMappings) throws Exception {
        throw new Exception("Resource type not supported for remapping");
    }*/
}
