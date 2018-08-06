package org.endeavourhealth.transform.common.resourceValidators;

import org.hl7.fhir.instance.model.Practitioner;
import org.hl7.fhir.instance.model.Resource;

import java.util.List;

public class ResourceValidatorPractitioner extends ResourceValidatorBase {
    @Override
    protected void validateResourceFields(Resource resource, List<String> validationErrors) {

        Practitioner practitioner = (Practitioner)resource;
        if (practitioner.hasPractitionerRole()) {
            if (practitioner.getPractitionerRole().size() > 1) {
                validationErrors.add("Practitioners have only have one role");
            }
        }
    }
}
