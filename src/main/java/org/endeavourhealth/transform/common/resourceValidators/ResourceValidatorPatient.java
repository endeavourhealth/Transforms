package org.endeavourhealth.transform.common.resourceValidators;

import org.hl7.fhir.instance.model.Patient;
import org.hl7.fhir.instance.model.Resource;

import java.util.List;

public class ResourceValidatorPatient extends ResourceValidatorBase {

    @Override
    protected void validateResourceFields(Resource resource, List<String> validationErrors) {

        Patient patient = (Patient)resource;
        if (!patient.hasManagingOrganization()) {
            validationErrors.add("Patient has no managing organisation (required for FHIR->Enterprise transform)");
        }
    }


}
