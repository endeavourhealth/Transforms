package org.endeavourhealth.transform.common.resourceValidators;

import org.hl7.fhir.instance.model.ReferralRequest;
import org.hl7.fhir.instance.model.Resource;

import java.util.List;

public class ResourceValidatorReferralRequest extends ResourceValidatorBase {
    @Override
    protected void validateResourceFields(Resource resource, List<String> validationErrors) {

        ReferralRequest referralRequest = (ReferralRequest) resource;
        if (referralRequest.hasServiceRequested()
                && referralRequest.getServiceRequested().size() > 1) {
            validationErrors.add("FHIR->Enterprise transform only supports ReferralRequests with a single service requested");
        }
    }
}
