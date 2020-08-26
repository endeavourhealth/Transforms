package org.endeavourhealth.transform.common.resourceValidators;

import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.Slot;

import java.util.List;

public class ResourceValidatorSlot extends ResourceValidatorBase {
    @Override
    protected void validateResourceFields(Resource resource, List<String> validationErrors) {
        //TODO - work out minimum requirements

            Slot slot = (Slot) resource;
            if (!slot.hasSchedule()) {
                validationErrors.add("FHIR->Enterprise transform requires schedule to have at least 1 Slot.");
            }
    }
}
