package org.endeavourhealth.transform.common.resourceValidators;

import org.hl7.fhir.instance.model.Procedure;
import org.hl7.fhir.instance.model.Resource;

import java.util.List;

public class ResourceValidatorProcedure extends ResourceValidatorBase {
    @Override
    protected void validateResourceFields(Resource resource, List<String> validationErrors) {

        Procedure procedure = (Procedure)resource;
        if (procedure.hasPerformer()
                && procedure.getPerformer().size() > 1) {
            validationErrors.add("FHIR->Enterprise transform only supports Procedures with a single performer");
        }
    }
}
