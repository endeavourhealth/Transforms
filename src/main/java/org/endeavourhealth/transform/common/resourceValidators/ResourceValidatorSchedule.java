package org.endeavourhealth.transform.common.resourceValidators;

import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.Schedule;

import java.util.List;

public class ResourceValidatorSchedule extends ResourceValidatorBase {
    @Override
    protected void validateResourceFields(Resource resource, List<String> validationErrors) {

        Schedule schedule = (Schedule)resource;
        if (schedule.hasType()
            && schedule.getType().size() > 1) {
            validationErrors.add("FHIR->Enterprise transform only supports Schedules with a single type");
        }
    }
}
