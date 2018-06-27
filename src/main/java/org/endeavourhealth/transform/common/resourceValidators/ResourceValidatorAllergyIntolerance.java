package org.endeavourhealth.transform.common.resourceValidators;

import org.hl7.fhir.instance.model.Resource;

import java.util.List;

public class ResourceValidatorAllergyIntolerance extends ResourceValidatorBase {

    @Override
    protected void validateResourceFields(Resource resource, List<String> validationErrors) {

        //TODO - work out what's a minimum
    }
}
