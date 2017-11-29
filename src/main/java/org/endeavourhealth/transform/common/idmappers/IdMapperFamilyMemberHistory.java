package org.endeavourhealth.transform.common.idmappers;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.hl7.fhir.instance.model.FamilyMemberHistory;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.Map;
import java.util.Set;

public class IdMapperFamilyMemberHistory extends BaseIdMapper {

    @Override
    public void getResourceReferences(Resource resource, Set<String> referenceValues) throws Exception {
        FamilyMemberHistory familyHistory = (FamilyMemberHistory)resource;
        super.addCommonResourceReferences(familyHistory, referenceValues);

        if (familyHistory.hasIdentifier()) {
            super.addIndentifierReferences(familyHistory.getIdentifier(), referenceValues);
        }
        if (familyHistory.hasPatient()) {
            super.addReference(familyHistory.getPatient(), referenceValues);
        }
    }

    @Override
    public void applyReferenceMappings(Resource resource, Map<String, String> mappings, boolean failForMissingMappings) throws Exception {
        FamilyMemberHistory familyHistory = (FamilyMemberHistory)resource;
        super.mapCommonResourceFields(familyHistory, mappings, failForMissingMappings);

        if (familyHistory.hasIdentifier()) {
            super.mapIdentifiers(familyHistory.getIdentifier(), mappings, failForMissingMappings);
        }
        if (familyHistory.hasPatient()) {
            super.mapReference(familyHistory.getPatient(), mappings, failForMissingMappings);
        }
    }

    @Override
    public String getPatientId(Resource resource) throws PatientResourceException {

        FamilyMemberHistory familyHistory = (FamilyMemberHistory)resource;
        if (familyHistory.hasPatient()) {
            return ReferenceHelper.getReferenceId(familyHistory.getPatient(), ResourceType.Patient);
        }
        return null;
    }

    /*@Override
    public boolean mapIds(Resource resource, UUID serviceId, UUID systemId, boolean mapResourceId) throws Exception {
        FamilyMemberHistory familyHistory = (FamilyMemberHistory)resource;

        if (familyHistory.hasIdentifier()) {
            super.mapIdentifiers(familyHistory.getIdentifier(), serviceId, systemId);
        }
        if (familyHistory.hasPatient()) {
            super.mapReference(familyHistory.getPatient(), serviceId, systemId);
        }

        return super.mapCommonResourceFields(familyHistory, serviceId, systemId, mapResourceId);
    }


    @Override
    public void remapIds(Resource resource, Map<String, String> idMappings) throws Exception {
        throw new Exception("Resource type not supported for remapping");
    }*/
}
