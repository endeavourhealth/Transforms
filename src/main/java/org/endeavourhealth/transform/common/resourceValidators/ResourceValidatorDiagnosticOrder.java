package org.endeavourhealth.transform.common.resourceValidators;

import org.hl7.fhir.instance.model.DiagnosticOrder;
import org.hl7.fhir.instance.model.Resource;

import java.util.List;

public class ResourceValidatorDiagnosticOrder extends ResourceValidatorBase {

    @Override
    protected void validateResourceFields(Resource resource, List<String> validationErrors) {

        DiagnosticOrder order = (DiagnosticOrder)resource;
        if (order.hasItem()
                && order.getItem().size() > 1) {
            validationErrors.add("FHIR->Enterprise transform only supports orders with a single item");
        }
    }
}
