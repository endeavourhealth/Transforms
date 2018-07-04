package org.endeavourhealth.transform.common.resourceValidators;

import org.hl7.fhir.instance.model.Duration;
import org.hl7.fhir.instance.model.MedicationOrder;
import org.hl7.fhir.instance.model.Resource;

import java.util.List;

public class ResourceValidatorMedicationOrder extends ResourceValidatorBase {
    @Override
    protected void validateResourceFields(Resource resource, List<String> validationErrors) {

        MedicationOrder order = (MedicationOrder) resource;

        if (order.hasDosageInstruction()
                && order.getDosageInstruction().size() > 1) {
            validationErrors.add("FHIR->Enterprise transform only supports MedicationOrders having one dosageInstruction");
        }

        if (order.hasDispenseRequest()) {
            MedicationOrder.MedicationOrderDispenseRequestComponent dispenseRequestComponent = order.getDispenseRequest();
            if (dispenseRequestComponent.hasExpectedSupplyDuration()) {
                Duration duration = dispenseRequestComponent.getExpectedSupplyDuration();
                if (duration.hasUnit()
                        && !duration.getUnit().equalsIgnoreCase("days")) {

                    validationErrors.add("FHIR->Enterprise transform only supports MedicationOrders with supply duration in days");
                }
            }
        }
    }
}
