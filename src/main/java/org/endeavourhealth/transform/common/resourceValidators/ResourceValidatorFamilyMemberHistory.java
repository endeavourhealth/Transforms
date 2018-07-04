package org.endeavourhealth.transform.common.resourceValidators;

import org.hl7.fhir.instance.model.FamilyMemberHistory;
import org.hl7.fhir.instance.model.Resource;

import java.util.List;

public class ResourceValidatorFamilyMemberHistory extends ResourceValidatorBase {

    @Override
    protected void validateResourceFields(Resource resource, List<String> validationErrors) {

        FamilyMemberHistory fh = (FamilyMemberHistory)resource;
        if (fh.hasCondition()
            && fh.getCondition().size() > 1) {
            validationErrors.add("FHIR->Enterprise transform only supports FamilyMemberHistory having one condition");
        }
    }
}
