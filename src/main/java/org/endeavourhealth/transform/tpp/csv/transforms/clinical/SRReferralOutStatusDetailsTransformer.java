package org.endeavourhealth.transform.tpp.csv.transforms.clinical;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppConfigListOption;
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
                    processRecord((SRReferralOutStatusDetails) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void processRecord(SRReferralOutStatusDetails parser, FhirResourceFiler fhirResourceFiler, TppCsvHelper csvHelper) {

        //if a status record is deleted, it will be replaced with a new status, and will
        //overwrite the current status. So we don't need to worry about the old status.
        CsvCell deleteData = parser.getRemovedData();
        if (deleteData != null && deleteData.getIntAsBoolean()) {
            return;
        }

        CsvCell referralIdCell = parser.getIDReferralOut();
        CsvCell referralStatusCell = parser.getStatusOfReferralOut();
        CsvCell dateCell = parser.getDateEvent();

        csvHelper.getReferralStatusCache().cacheReferralStatus(referralIdCell, dateCell, referralStatusCell);
    }

    /*private static void createResource(SRReferralOutStatusDetails parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper) throws Exception {

        CsvCell referralOutId = parser.getIDReferralOut();
        CsvCell patientId = parser.getIDPatient();
        CsvCell deleteData = parser.getRemovedData();

        //if a status record is deleted, it will be replaced with a new status, and will
        //overwrite the current status. So we don't need to worry about the old status.
        if (deleteData != null && deleteData.getIntAsBoolean()) {
            return;
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
        if (!TppCsvHelper.isEmptyOrNegative(referralStatus)) {

            TppConfigListOption tppConfigListOption = csvHelper.lookUpTppConfigListOption(referralStatus);
            if (tppConfigListOption != null) {
                String referralStatusDisplay = tppConfigListOption.getListOptionName();
                if (!Strings.isNullOrEmpty(referralStatusDisplay)) {
                    ReferralRequest.ReferralStatus status = convertReferralStatus(referralStatusDisplay);
                    referralRequestBuilder.setStatus(status, referralStatus);

                    //Update the referral description with status details
                    if (parser.getDateEvent().isEmpty()) {
                        //We sometimes get empty dates for completed. If we don't have a date and
                        // it's not completed it must be a draft entry which is default below anyway
                        referralRequestBuilder.setStatus(ReferralRequest.ReferralStatus.DRAFT, referralStatus);
                        return;
                    }
                    CsvCell referralStatusDate = parser.getDateEvent();
                    DateTimeType dateTimeType = new DateTimeType(referralStatusDate.getDateTime());
                    String displayDateTime = dateTimeType.toHumanDisplay();
                    String currentDescription = referralRequestBuilder.getDescription();
                    if (!Strings.isNullOrEmpty(currentDescription)) {
                        currentDescription = currentDescription.concat(". Status: " + displayDateTime + " - " + referralStatusDisplay);
                    } else {
                        currentDescription = "Status: " + displayDateTime + " - " + referralStatusDisplay;
                    }

                    referralRequestBuilder.setDescription(currentDescription, referralStatus);

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
        } else if (referralStatusDisplay.toLowerCase().contains("complete")) {
            return ReferralRequest.ReferralStatus.COMPLETED;
        }
            return ReferralRequest.ReferralStatus.DRAFT;
    }*/
}
