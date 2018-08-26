package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.publisherTransform.models.TppConfigListOption;
import org.endeavourhealth.transform.common.*;
import org.endeavourhealth.transform.common.resourceBuilders.ReferralRequestBuilder;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.referral.SRReferralOutStatusDetails;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ReferralRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void createResource(SRReferralOutStatusDetails parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper) throws Exception {

        CsvCell referralOutId = parser.getIDReferralOut();
        CsvCell patientId = parser.getIDPatient();
        CsvCell deleteData = parser.getRemovedData();

        if (deleteData != null && deleteData.getIntAsBoolean()) {
            // get previously filed resource for deletion
            if (referralOutId.isEmpty()) {
                referralOutId = parser.getRowIdentifier();
            }
            ReferralRequestBuilder referralRequestBuilder = csvHelper.getReferralRequestResourceCache().getReferralBuilder(referralOutId, csvHelper);
            if (referralRequestBuilder != null) {
                csvHelper.getReferralRequestResourceCache().addToDeletes(referralOutId, referralRequestBuilder);
            }
            return;
        }

        if (patientId.isEmpty()) {
            TransformWarnings.log(LOG, parser, "No Patient id in record for row: {},  file: {}",
                        parser.getRowIdentifier().getString(), parser.getFilePath());
            return;
//
//            if ((deleteData != null) && !deleteData.isEmpty() && !deleteData.getIntAsBoolean()) {
//                TransformWarnings.log(LOG, parser, "No Patient id in record for row: {},  file: {}",
//                        parser.getRowIdentifier().getString(), parser.getFilePath());
//                return;
//            } else {
//
//                // get previously filed resource for deletion
//                org.hl7.fhir.instance.model.ReferralRequest referralRequest
//                        = (org.hl7.fhir.instance.model.ReferralRequest) csvHelper.retrieveResource(referralOutId.getString(),
//                        ResourceType.ReferralRequest,
//                        fhirResourceFiler);
//
//                if (referralRequest != null) {
//                    ReferralRequestBuilder referralRequestBuilder = new ReferralRequestBuilder(referralRequest);
//                    fhirResourceFiler.deletePatientResource(parser.getCurrentState(), referralRequestBuilder);
//                    return;
//                }
//            }
        }

        ReferralRequestBuilder referralRequestBuilder = csvHelper.getReferralRequestResourceCache().getReferralBuilder(referralOutId, csvHelper);
        if (referralRequestBuilder == null) {
            return;
        }

        Reference patientReference = csvHelper.createPatientReference(patientId);
        if (referralRequestBuilder.isIdMapped()) {
            patientReference = IdHelper.convertLocallyUniqueReferenceToEdsReference(patientReference, fhirResourceFiler);
        }
        referralRequestBuilder.setPatient(patientReference, patientId);

        CsvCell referralStatus = parser.getStatusOfReferralOut();
        if (!referralStatus.isEmpty() && referralStatus.getLong() > 0) {

            TppConfigListOption tppConfigListOption = csvHelper.lookUpTppConfigListOption(referralStatus,parser);
            if (tppConfigListOption != null) {
                String referralStatusDisplay = tppConfigListOption.getListOptionName();
                if (!Strings.isNullOrEmpty(referralStatusDisplay)) {
                    ReferralRequest.ReferralStatus status = convertReferralStatus(referralStatusDisplay);
                    referralRequestBuilder.setStatus(status, referralStatus);

                    //Update the referral description with status details
                    CsvCell referralStatusDate = parser.getDateEvent();
                    DateTimeType dateTimeType = new DateTimeType(referralStatusDate.getDateTime());
                    if (dateTimeType != null) {
                        String displayDateTime = dateTimeType.toHumanDisplay();
                        String currentDescription = referralRequestBuilder.getDescription();
                        if (!Strings.isNullOrEmpty(currentDescription)) {
                            currentDescription = currentDescription.concat(". Status: " + displayDateTime + " - " + referralStatusDisplay);
                        } else {
                            currentDescription = "Status: " + displayDateTime + " - " + referralStatusDisplay;
                        }

                        referralRequestBuilder.setDescription(currentDescription, referralStatus);
                    }
                } else {

                    referralRequestBuilder.setStatus(ReferralRequest.ReferralStatus.DRAFT, referralStatus);
                }
            } else {

                referralRequestBuilder.setStatus(ReferralRequest.ReferralStatus.DRAFT, referralStatus);
            }
        } else {

            referralRequestBuilder.setStatus(ReferralRequest.ReferralStatus.DRAFT, referralStatus);
        }
    }

    private static ReferralRequest.ReferralStatus convertReferralStatus(String referralStatusDisplay) {

        if (referralStatusDisplay.toLowerCase().contains("waiting")) {
            return ReferralRequest.ReferralStatus.ACTIVE;
        } else
            return ReferralRequest.ReferralStatus.DRAFT;
    }
}
