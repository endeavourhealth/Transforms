package org.endeavourhealth.transform.common.idmappers;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.hl7.fhir.instance.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IdMapperQuestionnaireResponse extends BaseIdMapper {

    @Override
    public void getResourceReferences(Resource resource, Set<String> referenceValues) throws Exception {
        QuestionnaireResponse questionnaireResponse = (QuestionnaireResponse)resource;
        super.addCommonResourceReferences(questionnaireResponse, referenceValues);

        if (questionnaireResponse.hasIdentifier()) {

            //only a single identifier supported so pass as a list with single entry
            List<Identifier> identifiers = new ArrayList<>();
            identifiers.add(questionnaireResponse.getIdentifier());
            super.addIndentifierReferences(identifiers, referenceValues);
        }

        if (questionnaireResponse.hasSubject()) {
            super.addReference(questionnaireResponse.getSubject(), referenceValues);
        }

        if (questionnaireResponse.hasEncounter()) {
            super.addReference(questionnaireResponse.getEncounter(), referenceValues);
        }

        if (questionnaireResponse.hasAuthor()) {
            super.addReference(questionnaireResponse.getAuthor(), referenceValues);
        }
    }

    @Override
    public void applyReferenceMappings(Resource resource, Map<String, String> mappings, boolean failForMissingMappings) throws Exception {
        QuestionnaireResponse questionnaireResponse = (QuestionnaireResponse)resource;
        super.mapCommonResourceFields(questionnaireResponse, mappings, failForMissingMappings);

        if (questionnaireResponse.hasIdentifier()) {

            //only a single identifier supported so pass as a list with single entry
            List<Identifier> identifiers = new ArrayList<>();
            identifiers.add(questionnaireResponse.getIdentifier());
            super.mapIdentifiers(identifiers, mappings, failForMissingMappings);
        }

        if (questionnaireResponse.hasSubject()) {
            super.mapReference(questionnaireResponse.getSubject(), mappings, failForMissingMappings);
        }

        if (questionnaireResponse.hasEncounter()) {
            super.mapReference(questionnaireResponse.getEncounter(), mappings, failForMissingMappings);
        }

        if (questionnaireResponse.hasAuthor()) {
            super.mapReference(questionnaireResponse.getAuthor(), mappings, failForMissingMappings);
        }
    }

    @Override
    public String getPatientId(Resource resource) throws PatientResourceException {
        QuestionnaireResponse questionnaireResponse = (QuestionnaireResponse)resource;
        if (questionnaireResponse.hasSubject()) {
            return ReferenceHelper.getReferenceId(questionnaireResponse.getSubject(), ResourceType.Patient);
        }
        return null;
    }
}
