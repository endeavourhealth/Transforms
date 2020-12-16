package org.endeavourhealth.transform.common.resourceValidators;

import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.transform.ui.helpers.ExtensionHelper;
import org.hl7.fhir.instance.model.MedicationStatement;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.Type;

import java.util.List;

public class ResourceValidatorMedicationStatement extends ResourceValidatorBase {
    @Override
    protected void validateResourceFields(Resource resource, List<String> validationErrors) {

        MedicationStatement medicationStatement = (MedicationStatement)resource;
        if (medicationStatement.hasDosage()
                && medicationStatement.getDosage().size() > 1) {
            validationErrors.add("FHIR->Enterprise transform only supports MedicationStatements with a single dose");
        }

        Type authType = ExtensionConverter.findExtensionValue(medicationStatement, FhirExtensionUri.MEDICATION_AUTHORISATION_TYPE);
        if (authType == null) {
            validationErrors.add("FHIR->Enterprise transform requires an authorisation type");
        }

    }
}
