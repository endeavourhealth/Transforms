package org.endeavourhealth.transform.tpp.csv.transforms.referral;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.publisherTransform.models.TppConfigListOption;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.ReferralRequestBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.cache.ReferralRequestResourceCache;
import org.endeavourhealth.transform.tpp.csv.schema.referral.SRReferralOutStatusDetails;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.ReferralRequest;

import java.text.SimpleDateFormat;
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
        if (deleted.getIntAsBoolean()) {
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), referralRequestBuilder);
            return;
        }

        CsvCell referralStatus = parser.getStatusOfReferralOut();
        if (!referralStatus.isEmpty() && referralStatus.getLong() > 0) {

            TppConfigListOption tppConfigListOption = csvHelper.lookUpTppConfigListOption(referralStatus.getLong());
            String referralStatusDisplay = tppConfigListOption.getListOptionName();
            if (!Strings.isNullOrEmpty(referralStatusDisplay)) {
                ReferralRequest.ReferralStatus status = convertReferralStatus(referralStatusDisplay);
                referralRequestBuilder.setStatus(status, referralStatus);

                //Update the referral description with status details
                CsvCell referralStatusDate = parser.getDateEvent();
                DateTimeType dateTimeType = new DateTimeType(referralStatusDate.getDate());
                if (dateTimeType != null) {
                    SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
                    String displayDate = sdf.format(dateTimeType);
                    String currentDescription = referralRequestBuilder.getDescription();
                    if (!Strings.isNullOrEmpty(currentDescription)) {
                        currentDescription = currentDescription.concat(". Status: "+displayDate+" - "+referralStatusDisplay);
                    } else {
                        currentDescription = "Status: "+displayDate+" - "+referralStatusDisplay;
                    }

                    referralRequestBuilder.setDescription(currentDescription, referralStatus);
                }
            } else {
                referralRequestBuilder.setStatus(ReferralRequest.ReferralStatus.NULL, referralStatus);
            }
        } else {
            referralRequestBuilder.setStatus(ReferralRequest.ReferralStatus.NULL, referralStatus);
        }
    }

    private static ReferralRequest.ReferralStatus convertReferralStatus(String referralStatusDisplay) {

        if (referralStatusDisplay.toLowerCase().contains("waiting")) {
            return ReferralRequest.ReferralStatus.ACTIVE;
        } else
            return ReferralRequest.ReferralStatus.NULL;
    }
}
