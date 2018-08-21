package org.endeavourhealth.transform.tpp.cache;

import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.ReferralRequestBuilder;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.hl7.fhir.instance.model.ReferralRequest;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class ReferralRequestResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(ReferralRequestResourceCache.class);

    private Map<Long, ReferralRequestBuilder> referralRequestBuildersById = new HashMap<>();
    private Map<Long, ReferralRequestBuilder> referralRequestsToDelete = new HashMap<>();

    public ReferralRequestBuilder getReferralBuilder(CsvCell referralOutIdCell, TppCsvHelper csvHelper) throws Exception {

        Long key = referralOutIdCell.getLong();

        //if we've already decided we need to delete it, return null
        if (referralRequestsToDelete.containsKey(key)) {
            return null;
        }

        ReferralRequestBuilder referralRequestBuilder = referralRequestBuildersById.get(key);
        if (referralRequestBuilder == null) {

            ReferralRequest referralRequest = (ReferralRequest)csvHelper.retrieveResource(referralOutIdCell.getString(), ResourceType.ReferralRequest);
            if (referralRequest == null) {
                //if the ReferalRequest doesn't exist yet, create a new one
                referralRequestBuilder = new ReferralRequestBuilder();
                referralRequestBuilder.setId(referralOutIdCell.getString(), referralOutIdCell);

            } else {
                referralRequestBuilder = new ReferralRequestBuilder(referralRequest);
            }

            referralRequestBuildersById.put(key, referralRequestBuilder);
        }
        return referralRequestBuilder;
    }

    public void fileReferralRequestResources(FhirResourceFiler fhirResourceFiler) throws Exception {

        for (Long referralOutId: referralRequestBuildersById.keySet()) {
            ReferralRequestBuilder referralRequestBuilder = referralRequestBuildersById.get(referralOutId);

            boolean mapIds = !referralRequestBuilder.isIdMapped();
            fhirResourceFiler.savePatientResource(null, mapIds, referralRequestBuilder);
        }

        //clear down as everything has been saved
        referralRequestBuildersById.clear();

        for (Long referralId: referralRequestsToDelete.keySet()) {
            ReferralRequestBuilder referralRequestBuilder = referralRequestsToDelete.get(referralId);

            boolean mapIds = !referralRequestBuilder.isIdMapped();
            fhirResourceFiler.deletePatientResource(null, mapIds, referralRequestBuilder);
        }

        referralRequestsToDelete.clear();
    }

    public void addToDeletes(CsvCell referralOutId, ReferralRequestBuilder referralRequestBuilder) {
        Long key = referralOutId.getLong();
        referralRequestBuildersById.remove(key);
        referralRequestsToDelete.put(key, referralRequestBuilder);
    }
}
