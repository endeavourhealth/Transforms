package org.endeavourhealth.transform.common.resourceValidators;

import org.hl7.fhir.instance.model.MedicationStatement;
import org.hl7.fhir.instance.model.Resource;

import java.util.List;

public class ResourceValidatorMedicationStatement extends ResourceValidatorBase {
    @Override
    protected void validateResourceFields(Resource resource, List<String> validationErrors) {

        MedicationStatement medicationStatement = (MedicationStatement)resource;
        if (medicationStatement.hasDosage()
                && medicationStatement.getDosage().size() > 1) {
            validationErrors.add("FHIR->Enterprise transform only supports MedicationStatements with a single dose");
        }
    }
}
