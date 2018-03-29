package org.endeavourhealth.transform.tpp.csv.transforms.referral;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.ReferralRequestBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.cache.ReferralRequestResourceCache;
import org.endeavourhealth.transform.tpp.csv.schema.referral.SRReferralOutStatusDetails;
import org.hl7.fhir.instance.model.ReferralRequest;

import java.util.Map;

public class SRReferralOutStatusDetailsTransformer {

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRReferralOutStatusDetails.class);
        while (parser.nextRecord()) {

            try {
                createResource((SRReferralOutStatusDetails) parser, fhirResourceFiler, csvHelper);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    private static void createResource(SRReferralOutStatusDetails parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper) throws Exception {

        CsvCell referralOutId = parser.getIDReferralOut();
        CsvCell patientId = parser.getIDPatient();

        ReferralRequestBuilder referralRequestBuilder
                = ReferralRequestResourceCache.getReferralBuilder(referralOutId, patientId, csvHelper, fhirResourceFiler);

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        CsvCell deleted = parser.getRemovedData();
        if (deleted.getBoolean()) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), referralRequestBuilder);
            return;
        }

        CsvCell referralStatus = parser.getStatusOfReferralOut();
        if (!referralStatus.isEmpty() && referralStatus.getLong() > 0) {

            //TODO:  lookup SRConfigureListOption then convert
            String referralStatusDisplay = "";

            ReferralRequest.ReferralStatus status = convertReferralStatus(referralStatusDisplay);
            referralRequestBuilder.setStatus(status, referralStatus);
        } else {
            referralRequestBuilder.setStatus(ReferralRequest.ReferralStatus.NULL, referralStatus);
        }
    }

    private static ReferralRequest.ReferralStatus convertReferralStatus(String referralStatusDisplay) throws Exception {

        //TODO - example status from code lookup
        if (referralStatusDisplay.equalsIgnoreCase("TODO")) {
            return ReferralRequest.ReferralStatus.ACTIVE;
        } else
            return ReferralRequest.ReferralStatus.NULL;
    }
}
