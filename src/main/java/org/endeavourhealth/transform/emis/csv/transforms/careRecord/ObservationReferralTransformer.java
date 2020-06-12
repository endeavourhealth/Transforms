package org.endeavourhealth.transform.emis.csv.transforms.careRecord;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.schema.ReferralPriority;
import org.endeavourhealth.common.fhir.schema.ReferralRequestSendMode;
import org.endeavourhealth.common.fhir.schema.ReferralType;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ReferralRequestBuilder;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.helpers.EmisMappingHelper;
import org.endeavourhealth.transform.emis.csv.schema.careRecord.ObservationReferral;
import org.hl7.fhir.instance.model.Identifier;
import org.hl7.fhir.instance.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ObservationReferralTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(ObservationReferralTransformer.class);

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        ObservationReferral parser = (ObservationReferral) parsers.get(ObservationReferral.class);
        while (parser != null && parser.nextRecord()) {

            try {
                if (csvHelper.shouldProcessRecord(parser)) {
                    createResource(parser, fhirResourceFiler, csvHelper);
                }
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void createResource(ObservationReferral parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       EmisCsvHelper csvHelper) throws Exception {

        ReferralRequestBuilder referralRequestBuilder = new ReferralRequestBuilder();

        CsvCell observationGuid = parser.getObservationGuid();
        CsvCell patientGuid = parser.getPatientGuid();

        EmisCsvHelper.setUniqueId(referralRequestBuilder, patientGuid, observationGuid);

        Reference patientReference = EmisCsvHelper.createPatientReference(patientGuid);
        referralRequestBuilder.setPatient(patientReference, patientGuid);

        CsvCell ubrn = parser.getReferralUBRN();
        if (!ubrn.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(referralRequestBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.OFFICIAL);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_UBRN);
            identifierBuilder.setValue(ubrn.getString(), ubrn);
        }

        CsvCell urgencyCell = parser.getReferralUrgency();
        if (!urgencyCell.isEmpty()) {

            CodeableConceptBuilder ccb = new CodeableConceptBuilder(referralRequestBuilder, CodeableConceptBuilder.Tag.Referral_Request_Priority);

            //always carry over the text
            String urgencyDesc = urgencyCell.getString();
            ccb.setText(urgencyDesc, urgencyCell);

            //see if we've mapped it to a valueset value
            ReferralPriority mappedPriority = EmisMappingHelper.findReferralPriority(urgencyDesc);
            if (mappedPriority != null) {
                ccb.addCoding(mappedPriority.getSystem()); //the URL for the priority system
                ccb.setCodingCode(mappedPriority.getCode());
                ccb.setCodingDisplay(mappedPriority.getDescription());

            } else {
                TransformWarnings.log(LOG, csvHelper, "Unmapped EMIS referral priority {}", urgencyDesc);
            }
        }

        CsvCell serviceType = parser.getReferralServiceType();
        if (!serviceType.isEmpty()) {
            ReferralType type = EmisMappingHelper.findReferralType(serviceType.getString());
            if (type != null) {
                referralRequestBuilder.setType(type, serviceType);

            } else {
                //if the type hasn't been mapped to a proper type, carry over as text
                referralRequestBuilder.setTypeFreeText(serviceType.getString(), serviceType);
            }
        }

        CsvCell mode = parser.getReferralMode();
        if (!mode.isEmpty()) {
            ReferralRequestSendMode fhirMode = EmisMappingHelper.findReferralMode(mode.getString());
            if (fhirMode != null) {
                referralRequestBuilder.setMode(fhirMode, mode);

            } else {
                //if the mode hasn't been mapped to a proper type, carry over as text
                referralRequestBuilder.setModeFreeText(mode.getString(), mode);
            }
        }

        CsvCell recipientOrgGuid = parser.getReferralTargetOrganisationGuid();
        //the spec. states that this value will always be present, but there's some live data with a missing value
        if (!recipientOrgGuid.isEmpty()) {
            Reference orgReference = EmisCsvHelper.createOrganisationReference(recipientOrgGuid);
            referralRequestBuilder.addRecipient(orgReference, recipientOrgGuid);
        }

        //the below values are defined in the spec., but the spec also states that they'll be empty, so
        //none of the below will probably be used
        CsvCell sendingOrgGuid = parser.getReferralSourceOrganisationGuid();
        if (sendingOrgGuid.isEmpty()) {
            //in the absence of any data, treat the referral as though it was FROM this service so long as it wasn't TO this service
            CsvCell orgGuid = parser.getOrganisationGuid();
            if (!orgGuid.equalsValue(recipientOrgGuid)) {
                sendingOrgGuid = orgGuid;
            }
        }

        if (!sendingOrgGuid.isEmpty()) {
            Reference orgReference = EmisCsvHelper.createOrganisationReference(sendingOrgGuid);
            referralRequestBuilder.setRequester(orgReference, sendingOrgGuid);
        }

        //although the columns exist in the CSV, the spec. states that they'll always be empty
        //ReferralReceivedDateTime
        //ReferralEndDate
        //ReferralSourceId - links to Coding_ClinicalCode
        //ReferralReasonCodeId - links to Coding_ClinicalCode
        //ReferringCareProfessionalStaffGroupCodeId - links to Coding_ClinicalCode
        //ReferralEpisodeRTTMeasurementTypeId - links to Coding_ClinicalCode
        //ReferralEpisodeClosureDate
        //ReferralEpisodeDischargeLetterIssuedDate
        //ReferralClosureReasonCodeId - links to Coding_ClinicalCode

        //unlike other resources, we don't save the Referral immediately, as there's data we
        //require on the corresponding row in the Observation file. So cache in the helper
        //and we'll finish the job when we get to that.
        csvHelper.cacheReferral(observationGuid, patientGuid, referralRequestBuilder);

    }

    /*private static ReferralType convertType(String type) throws Exception {

        if (type.equalsIgnoreCase("Unknown")) {
            return ReferralType.UNKNOWN;

        } else if (type.equalsIgnoreCase("Assessment")) {
            return ReferralType.ASSESSMENT;

        } else if (type.equalsIgnoreCase("Investigation")) {
            return ReferralType.INVESTIGATION;

        } else if (type.equalsIgnoreCase("Management advice")) {
            return ReferralType.MANAGEMENT_ADVICE;

        } else if (type.equalsIgnoreCase("Patient reassurance")) {
            return ReferralType.PATIENT_REASSURANCE;

        } else if (type.equalsIgnoreCase("Self referral")) {
            return ReferralType.SELF_REFERRAL;

        } else if (type.equalsIgnoreCase("Treatment")) {
            return ReferralType.TREATMENT;

        } else if (type.equalsIgnoreCase("Outpatient")) {
            return ReferralType.OUTPATIENT;

        } else if (type.equalsIgnoreCase("Community Care")) {
            return ReferralType.COMMUNITY_CARE;

        } else if (type.equalsIgnoreCase("Performance of a procedure / operation")) {
            return ReferralType.PROCEDURE;

        } else if (type.equalsIgnoreCase("Admission")) {
            return ReferralType.ADMISSION;

        } else if (type.equalsIgnoreCase("Day Care")) {
            return ReferralType.DAY_CARE;

        } else if (type.equalsIgnoreCase("Assessment & Education")) {
            return ReferralType.ASSESSMENT_AND_EDUCATION;

        } else {
            return null;
        }
    }

    private static ReferralPriority convertUrgency(String urgency) throws Exception {

        //EMIS urgencies based on EMIS Open format (VocReferralUrgency)
        if (urgency.equalsIgnoreCase("Routine")) {
            return ReferralPriority.ROUTINE;

        } else if (urgency.equalsIgnoreCase("Urgent")) {
            return ReferralPriority.URGENT;

        } else if (urgency.equalsIgnoreCase("2 Week Wait")) {
            return ReferralPriority.TWO_WEEK_WAIT;

        } else if (urgency.equalsIgnoreCase("Soon")) {
            return ReferralPriority.SOON;

        } else {
            return null;
        }
    }

    private static ReferralRequestSendMode convertMode(String mode) {
        try {
            return ReferralRequestSendMode.fromDescription(mode);

        } catch (IllegalArgumentException ex) {
            //if it can't be mapped directly by name, we can attempt an alternative lookup
            if (mode.equalsIgnoreCase("Choose and Book")) {
                //ERS is the new name for Choose and Book
                return ReferralRequestSendMode.ERS;

            } else {
                return null;
            }
        }
    }*/
}
