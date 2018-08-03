package org.endeavourhealth.transform.common.resourceValidators;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.hl7.fhir.instance.model.Address;
import org.hl7.fhir.instance.model.Identifier;
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

        if (patient.hasIdentifier()) {
            for (Identifier identifier: patient.getIdentifier()) {
                if (identifier.getSystem().equals(FhirIdentifierUri.IDENTIFIER_SYSTEM_NHSNUMBER)) {
                    String nhsNumber = identifier.getValue();
                    if (nhsNumber.contains(" ")) {
                        validationErrors.add("NHS number contains spaces");
                    } else if (nhsNumber.contains("-")) {
                        validationErrors.add("NHS number contains dashes");
                    }

                    if (identifier.getUse() != Identifier.IdentifierUse.OFFICIAL) {
                        validationErrors.add("NHS number should have use = OFFICIAL");
                    }
                }
            }
        }
    }


}
