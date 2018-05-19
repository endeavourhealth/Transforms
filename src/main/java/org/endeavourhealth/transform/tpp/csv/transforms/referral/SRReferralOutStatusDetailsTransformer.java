package org.endeavourhealth.transform.tpp.csv.transforms.referral;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.publisherTransform.models.TppConfigListOption;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.ReferralRequestBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.cache.ReferralRequestResourceCache;
import org.endeavourhealth.transform.tpp.csv.schema.referral.SRReferralOutStatusDetails;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.ReferralRequest;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Map;

public class SRReferralOutStatusDetailsTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(SRReferralOutStatusDetailsTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRReferralOutStatusDetails.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((SRReferralOutStatusDetails) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }

    private static void createResource(SRReferralOutStatusDetails parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper) throws Exception {

        CsvCell referralOutId = parser.getIDReferralOut();
        CsvCell patientId = parser.getIDPatient();
        CsvCell deleteData = parser.getRemovedData();

        if (patientId.isEmpty()) {

            if (!deleteData.isEmpty() && !deleteData.getIntAsBoolean()) {
                TransformWarnings.log(LOG, parser, "No Patient id in record for row: {},  file: {}",
                        parser.getRowIdentifier().getString(), parser.getFilePath());
                return;
            } else {

                // get previously filed resource for deletion
                org.hl7.fhir.instance.model.ReferralRequest referralRequest
                        = (org.hl7.fhir.instance.model.ReferralRequest) csvHelper.retrieveResource(referralOutId.getString(),
                        ResourceType.ReferralRequest,
                        fhirResourceFiler);

                if (referralRequest != null) {
                    ReferralRequestBuilder referralRequestBuilder = new ReferralRequestBuilder(referralRequest);
                    fhirResourceFiler.deletePatientResource(parser.getCurrentState(), referralRequestBuilder);
                    return;
                }
            }
        }

        ReferralRequestBuilder referralRequestBuilder
                = ReferralRequestResourceCache.getReferralBuilder(referralOutId, patientId, csvHelper, fhirResourceFiler);

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
