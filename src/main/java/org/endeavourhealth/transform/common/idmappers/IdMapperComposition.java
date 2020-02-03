package org.endeavourhealth.transform.common.idmappers;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.hl7.fhir.instance.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IdMapperComposition extends BaseIdMapper {

    @Override
    public void getResourceReferences(Resource resource, Set<String> referenceValues) throws Exception {
        Composition composition = (Composition)resource;
        super.addCommonResourceReferences(composition, referenceValues);

        if (composition.hasIdentifier()) {

            //only a single identifier supported so pass as a list with single entry
            List<Identifier> identifiers = new ArrayList<>();
            identifiers.add(composition.getIdentifier());
            super.addIndentifierReferences(identifiers, referenceValues);
        }

        if (composition.hasSubject()) {
            super.addReference(composition.getSubject(), referenceValues);
        }
    }

    @Override
    public void applyReferenceMappings(Resource resource, Map<String, String> mappings, boolean failForMissingMappings) throws Exception {
        Composition composition = (Composition)resource;
        super.mapCommonResourceFields(composition, mappings, failForMissingMappings);

        if (composition.hasIdentifier()) {

            //only a single identifier supported so pass as a list with single entry
            List<Identifier> identifiers = new ArrayList<>();
            identifiers.add(composition.getIdentifier());
            super.mapIdentifiers(identifiers, mappings, failForMissingMappings);
        }

        if (composition.hasSubject()) {
            super.mapReference(composition.getSubject(), mappings, failForMissingMappings);
        }
    }

    @Override
    public String getPatientId(Resource resource) throws PatientResourceException {
        Composition composition = (Composition)resource;
        if (composition.hasSubject()) {
            return ReferenceHelper.getReferenceId(composition.getSubject(), ResourceType.Patient);
        }
        return null;
    }
}
