package org.endeavourhealth.transform.tpp.cache;

import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.ReferralRequestBuilder;
import org.endeavourhealth.transform.common.resourceValidators.ResourceValidatorReferralRequest;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReferralRequestResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(ReferralRequestResourceCache.class);

    private static Map<Long, ReferralRequestBuilder> referralRequestBuildersById = new HashMap<>();

    public static ReferralRequestBuilder getReferralBuilder(CsvCell referralOutIdCell,
                                                            CsvCell patientIdCell,
                                                            TppCsvHelper csvHelper,
                                                            FhirResourceFiler fhirResourceFiler) throws Exception {

        ReferralRequestBuilder referralRequestBuilder = referralRequestBuildersById.get(referralOutIdCell.getLong());
        if (referralRequestBuilder == null) {

            org.hl7.fhir.instance.model.ReferralRequest referralRequest
                    = (org.hl7.fhir.instance.model.ReferralRequest)csvHelper.retrieveResource(referralOutIdCell.getString(), ResourceType.ReferralRequest);
            if (referralRequest == null) {
                //if the ReferalRequest doesn't exist yet, create a new one
                referralRequestBuilder = new ReferralRequestBuilder();
                referralRequestBuilder.setId(referralOutIdCell.getString(), referralOutIdCell);

            } else {
                referralRequestBuilder = new ReferralRequestBuilder(referralRequest);
            }

            referralRequestBuildersById.put(referralOutIdCell.getLong(), referralRequestBuilder);
        }
        return referralRequestBuilder;
    }

    public static void fileReferralRequestResources(FhirResourceFiler fhirResourceFiler) throws Exception {
        int count = 0;
        for (Long referralOutId: referralRequestBuildersById.keySet()) {
            ReferralRequestBuilder referralRequestBuilder = referralRequestBuildersById.get(referralOutId);
            ResourceValidatorReferralRequest validator = new ResourceValidatorReferralRequest();
            boolean mapIds = !referralRequestBuilder.isIdMapped();
            List<String> problems = new ArrayList<String>();
            validator.validateResourceSave(referralRequestBuilder.getResource(),fhirResourceFiler.getServiceId(),mapIds, problems);
            if (problems.isEmpty()) {
                fhirResourceFiler.savePatientResource(null, mapIds, referralRequestBuilder);
            } else {
                LOG.warn("TPPValidator: Autoset resource boolean. Referal request id: {}. Filed {}", referralRequestBuilder.getResourceId(), count );

                fhirResourceFiler.savePatientResource(null,!mapIds,referralRequestBuilder);
            }
            count++;
        }

        //clear down as everything has been saved
        referralRequestBuildersById.clear();
    }
}
