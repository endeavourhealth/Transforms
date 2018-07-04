package org.endeavourhealth.transform.common.resourceValidators;

import org.hl7.fhir.instance.model.*;

import java.util.List;

public class ResourceValidatorObservation extends ResourceValidatorBase {
    @Override
    protected void validateResourceFields(Resource resource, List<String> validationErrors) {

        Observation observation = (Observation)resource;
        if (observation.hasValue()) {
            Type value = observation.getValue();
            if (value instanceof Quantity
                || value instanceof DateTimeType
                || value instanceof StringType
                || value instanceof CodeableConcept) {
                //OK

            } else {
                validationErrors.add("FHIR->Enterprise transform does not support values of type " + value.getClass().getName());
            }
        }
    }
}
