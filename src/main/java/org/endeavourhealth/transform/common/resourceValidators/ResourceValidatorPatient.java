package org.endeavourhealth.transform.common.resourceValidators;

import org.hl7.fhir.instance.model.Address;
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

        if (patient.hasAddress()) {
            for (Address address: patient.getAddress()) {
                if (address.hasPostalCode()) {
                    String postcode = address.getPostalCode();
                    if (postcode.indexOf(" ") > -1) {
                        validationErrors.add("Postcode contains spaces");
                    }
                }
            }
        }
    }


}
