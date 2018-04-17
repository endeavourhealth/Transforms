package org.endeavourhealth.transform.common.idmappers;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.hl7.fhir.instance.model.Flag;
import org.hl7.fhir.instance.model.Immunization;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.Map;
import java.util.Set;

public class IdMapperFlag extends BaseIdMapper {

    @Override
    public void getResourceReferences(Resource resource, Set<String> referenceValues) throws Exception {
        Flag flag = (Flag)resource;
        super.addCommonResourceReferences(flag, referenceValues);

        if (flag.hasIdentifier()) {
            super.addIndentifierReferences(flag.getIdentifier(), referenceValues);
        }

        if (flag.hasSubject()) {
            super.addReference(flag.getSubject(), referenceValues);
        }

        if (flag.hasEncounter()) {
            super.addReference(flag.getEncounter(), referenceValues);
        }

        if (flag.hasAuthor()) {
            super.addReference(flag.getAuthor(), referenceValues);
        }
    }

    @Override
    public void applyReferenceMappings(Resource resource, Map<String, String> mappings, boolean failForMissingMappings) throws Exception {
        Flag flag = (Flag)resource;
        super.mapCommonResourceFields(flag, mappings, failForMissingMappings);

        if (flag.hasIdentifier()) {
            super.mapIdentifiers(flag.getIdentifier(), mappings, failForMissingMappings);
        }

        if (flag.hasSubject()) {
            super.mapReference(flag.getSubject(), mappings, failForMissingMappings);
        }

        if (flag.hasEncounter()) {
            super.mapReference(flag.getEncounter(), mappings, failForMissingMappings);
        }

        if (flag.hasAuthor()) {
            super.mapReference(flag.getAuthor(), mappings, failForMissingMappings);
        }
    }

    @Override
    public String getPatientId(Resource resource) throws PatientResourceException {
        Flag flag = (Flag)resource;
        if (flag.hasSubject()) {
            return ReferenceHelper.getReferenceId(flag.getSubject(), ResourceType.Patient);
        }
        return null;
    }
}
