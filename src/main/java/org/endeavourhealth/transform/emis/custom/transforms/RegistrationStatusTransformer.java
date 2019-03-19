package org.endeavourhealth.transform.emis.custom.transforms;

import org.endeavourhealth.common.fhir.schema.RegistrationType;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.custom.helpers.EmisCustomCsvHelper;
import org.endeavourhealth.transform.emis.custom.schema.RegistrationStatus;

public class RegistrationStatusTransformer {

    
    public static void transform(AbstractCsvParser parser,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCustomCsvHelper csvHelper) throws Exception {

        while (parser.nextRecord()) {

            try {
                processRecord((RegistrationStatus) parser, fhirResourceFiler, csvHelper);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void processRecord(RegistrationStatus parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      EmisCustomCsvHelper csvHelper) throws Exception {

        CsvCell patientGuidCell = parser.getPatientGuid();
        CsvCell dateTimeCell = parser.getDate();
        CsvCell regStatusCell = parser.getRegistrationStatus();
        CsvCell regTypeCell = parser.getRegistrationType();
        CsvCell organisationGuidCell = parser.getOrganisationGuid();
        //CsvCell processingOrderCell = parser.getProcessingOrder();

        csvHelper.cacheRegStatus(patientGuidCell, regStatusCell, dateTimeCell, regTypeCell, organisationGuidCell);
    }



    public static org.endeavourhealth.common.fhir.schema.RegistrationStatus convertRegistrationStatus(Integer obj) throws Exception {
        int value = obj.intValue();


        switch (value) {
            case 1:
                return org.endeavourhealth.common.fhir.schema.RegistrationStatus.PRE_REGISTERED_PRESENTED;
            case 2:
                return org.endeavourhealth.common.fhir.schema.RegistrationStatus.PRE_REGISTERED_MEDICAL_CARD_RECEIVED;
            case 3:
                return org.endeavourhealth.common.fhir.schema.RegistrationStatus.PRE_REGISTERED_FP1_SUBMITTED;
            case 4:
                return org.endeavourhealth.common.fhir.schema.RegistrationStatus.REGISTERED;
            case 5:
                return org.endeavourhealth.common.fhir.schema.RegistrationStatus.REGISTERED_RECORD_SENT_FROM_FHSA;
            case 6:
                return org.endeavourhealth.common.fhir.schema.RegistrationStatus.REGISTERED_RECORD_RECEIVED_FROM_FHSA;
            case 7:
                return org.endeavourhealth.common.fhir.schema.RegistrationStatus.REGISTERED_LEFT_PRACTICE_STILL_REGISTERED;
            case 8:
                return org.endeavourhealth.common.fhir.schema.RegistrationStatus.REGISTERED_CORRECTLY;
            case 9:
                return org.endeavourhealth.common.fhir.schema.RegistrationStatus.REGISTERED_TEMPORARY_SHORT_STAY;
            case 10:
                return org.endeavourhealth.common.fhir.schema.RegistrationStatus.REGISTERED_TEMPORARY_LONG_STAY;
            case 11:
                return org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_DEATH;
            case 12:
                return org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_DEATH_NOTIFICATION;
            case 13:
                return org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_RECORD_REQUESTED_BY_FHSA;
            case 14:
                return org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_REMOVAL_TO_NEW_HA;
            case 15:
                return org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_INTERNAL_TRANSFER;
            case 16:
                return org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_MENTAL_HOSPITAL;
            case 17:
                return org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_EMBARKATION;
            case 18:
                return org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_NEW_HA_SAME_GP;
            case 19:
                return org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_ADOPTED_CHILD;
            case 20:
                return org.endeavourhealth.common.fhir.schema.RegistrationStatus.REGISTERED_SERVICES;
            case 21:
                return org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_AT_GP_REQUEST;
            case 22:
                return org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_REGISTRATION_CANCELLED;
            case 23:
                return org.endeavourhealth.common.fhir.schema.RegistrationStatus.REGISTERED_SERVICES_DEPENDENT;
            case 24:
                return org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_AT_PATIENT_REQUEST;
            case 25:
                return org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_OTHER_REASON;
            case 26:
                return org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_MAIL_RETURNED_UNDELIVERED;
            case 27:
                return org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_INTERNAL_TRANSFER_ADDRESS_CHANGE;
            case 28:
                return org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_INTERNAL_TRANSFER_WITHIN_PARTNERSHIP;
            case 29:
                return org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_MAIL_STATES_GONE_AWAY;
            case 30:
                return org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_OUTSIDE_OF_AREA;
            case 31:
                return org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_NO_LONGER_RESIDENT;
            case 32:
                return org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_VIA_SCREENING_SYSTEM;
            case 33:
                return org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_VIA_VACCINATION_DATA;
            case 34:
                return org.endeavourhealth.common.fhir.schema.RegistrationStatus.REGISTERED_REMOVAL_FROM_RESIDENTIAL_INSITUTE;
            case 35:
                return org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_RECORDS_SENT_BACK_TO_FHSA;
            case 36:
                return org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_RECORDS_RECEIVED_BY_FHSA;
            case 37:
                return org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_REGISTRATION_EXPIRED;
            default:
                throw new TransformException("Unsupported registration status " + value);
        }
    }

    /**
     * certain statuses indicate, or are part of, the deduction process
     */
    public static boolean isDeductionRegistrationStatus(org.endeavourhealth.common.fhir.schema.RegistrationStatus status) {

        return status == org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_DEATH
                || status == org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_DEATH_NOTIFICATION
                || status == org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_RECORD_REQUESTED_BY_FHSA
                || status == org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_REMOVAL_TO_NEW_HA
                || status == org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_INTERNAL_TRANSFER
                || status == org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_MENTAL_HOSPITAL
                || status == org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_EMBARKATION
                || status == org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_NEW_HA_SAME_GP
                || status == org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_ADOPTED_CHILD
                || status == org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_AT_GP_REQUEST
                || status == org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_REGISTRATION_CANCELLED
                || status == org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_AT_PATIENT_REQUEST
                || status == org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_OTHER_REASON
                || status == org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_MAIL_RETURNED_UNDELIVERED
                || status == org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_INTERNAL_TRANSFER_ADDRESS_CHANGE
                || status == org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_INTERNAL_TRANSFER_WITHIN_PARTNERSHIP
                || status == org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_MAIL_STATES_GONE_AWAY
                || status == org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_OUTSIDE_OF_AREA
                || status == org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_NO_LONGER_RESIDENT
                || status == org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_VIA_SCREENING_SYSTEM
                || status == org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_VIA_VACCINATION_DATA
                || status == org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_RECORDS_SENT_BACK_TO_FHSA
                || status == org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_RECORDS_RECEIVED_BY_FHSA
                || status == org.endeavourhealth.common.fhir.schema.RegistrationStatus.DEDUCTED_REGISTRATION_EXPIRED;
    }

    public static RegistrationType convertRegistrationType(Integer obj) throws Exception {
        int value = obj.intValue();

        if (value == 1) { //Emergency
            return RegistrationType.EMERGENCY;
        } else if (value == 2) { //Immediately Necessary
            return RegistrationType.IMMEDIATELY_NECESSARY;
        } else if (value == 3) { //Private
            return RegistrationType.PRIVATE;
        } else if (value == 4) { //Regular
            return RegistrationType.REGULAR_GMS;
        } else if (value == 5) { //Temporary
            return RegistrationType.TEMPORARY;
        } else if (value == 6) { //Community Registered
            return RegistrationType.COMMUNITY;
        } else if (value == 7) { //Dummy
            return RegistrationType.DUMMY;
        } else if (value == 8) { //Other
            return RegistrationType.OTHER;
        } else if (value == 12) { //Walk-In Patient
            return RegistrationType.WALK_IN;
        } else if (value == 13) { //Minor Surgery
            return RegistrationType.MINOR_SURGERY;
        } else if (value == 11) { //Child Health Services
            return RegistrationType.CHILD_HEALTH_SURVEILLANCE;
        } else if (value == 9) { //Contraceptive Services
            return RegistrationType.CONTRACEPTIVE_SERVICES;
        } else if (value == 10) { //Maternity Services
            return RegistrationType.MATERNITY_SERVICES;
        } else if (value == 16) { //Yellow Fever
            return RegistrationType.YELLOW_FEVER;
        } else if (value == 15) { //Pre Registration
            return RegistrationType.PRE_REGISTRATION;
        } else if (value == 14) { //Sexual Health
            return RegistrationType.SEXUAL_HEALTH;
        } else if (value == 24) { //Vasectomy
            return RegistrationType.VASECTOMY;
        } else if (value == 28) { //Out of Hours
            return RegistrationType.OUT_OF_HOURS;
        } else {
            throw new TransformException("Unsupported registration type " + value);
        }

    }

}
