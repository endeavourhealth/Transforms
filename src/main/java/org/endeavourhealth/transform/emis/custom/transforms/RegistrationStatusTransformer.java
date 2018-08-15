package org.endeavourhealth.transform.emis.custom.transforms;

import org.endeavourhealth.common.fhir.schema.RegistrationType;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.emis.custom.schema.RegistrationStatus;

public class RegistrationStatusTransformer {

    public static void transform(AbstractCsvParser parser,
                                 FhirResourceFiler fhirResourceFiler) throws Exception {

        while (parser.nextRecord()) {

            try {
                processRecord((RegistrationStatus) parser, fhirResourceFiler);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void processRecord(RegistrationStatus parser,
                                      FhirResourceFiler fhirResourceFiler) throws Exception {

       /* CsvCell patientGuidCell = parser.getPatientGuid();
        String patientGuid = patientGuidCell.getString();

        //the patient GUID in the standard extract files is in upper case and
        //has curly braces around it, so we need to ensure this is the same
        patientGuid = "{" + patientGuid.toUpperCase() + "}";

        EpisodeOfCare episodeOfCare = (EpisodeOfCare) csvHelper.retrieveResource(patientGuid, ResourceType.EpisodeOfCare);
        EpisodeOfCareBuilder episodeBuilder = new EpisodeOfCareBuilder(episodeOfCare);

        //only carry over the registration type from this file if we've not got it on the episode
        if (!episodeBuilder.hasRegistrationType()) {
            CsvCell registrationTypeIdCell = parser.getRegistrationTypeId();
            RegistrationType registrationType = convertRegistrationType(registrationTypeIdCell.getInt());
            episodeBuilder.setRegistrationType(registrationType, registrationTypeIdCell);
        }

        CsvCell registrationStatusIdCell = parser.getRegistrationStatusId();
        String registrationStatus = convertRegistrationStatus(registrationStatusIdCell.getInt());
        episodeBuilder.setMedicalRecordStatus(registrationStatus, registrationStatusIdCell);

        //TODO - should we keep the full registration status on the episode???
        //TODO - should we carry over the concept of a registration status being "active" or not? Status 1-3 are NOT counted as active?
        //TODO - how to handle multiple records per patient? Make this like a pre-transformer or something?

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), false, episodeBuilder);*/
    }

    private static String convertRegistrationStatus(Integer obj) throws Exception {
        int value = obj.intValue();

        switch (value) {
            case 1:
                return "Patient has presented";
            case 2:
                return "Medical card (FP4) received";
            case 3:
                return "Application Form FP1 submitted";
            case 4:
                return "Notification of registration";
            case 5:
                return "Medical record sent by FHSA";
            case 6:
                return "Record Received";
            case 7:
                return "Left Practice. Still Registered";
            case 8:
                return "Correctly registered";
            case 9:
                return "Short stay";
            case 10:
                return "Long stay";
            case 11:
                return "Death";
            case 12:
                return "Dead (Practice notification)";
            case 13:
                return "Record Requested by FHSA";
            case 14:
                return "Removal to New HA/HB";
            case 15:
                return "Internal transfer";
            case 16:
                return "Mental hospital";
            case 17:
                return "Embarkation";
            case 18:
                return "New HA/HB - same GP";
            case 19:
                return "Adopted child";
            case 20:
                return "Services";
            case 21:
                return "Deduction at GP's request";
            case 22:
                return "Registration cancelled";
            case 23:
                return "Service dependant";
            case 24:
                return "Deduction at patient's request";
            case 25:
                return "Other reason";
            case 26:
                return "Returned undelivered";
            case 27:
                return "Internal transfer - address change";
            case 28:
                return "Internal transfer within partnership";
            case 29:
                return "Correspondence states 'gone away'";
            case 30:
                return "Practice advise outside of area";
            case 31:
                return "Practice advise patient no longer resident";
            case 32:
                return "Practice advise removal via screening system";
            case 33:
                return "Practice advise removal via vaccination data";
            case 34:
                return "Removal from Residential Institute";
            case 35:
                return "Records sent back to FHSA";
            case 36:
                return "Records received by FHSA";
            case 37:
                return "Registration expired";
            default:
                throw new TransformException("Unsupported registration status " + value);
        }
        
        /*
        NOTE: the below are also registration statuses, but no known patients have them
        38	All records removed
        39	Untraced-outwith HB
        40	Multiple Transfer
        41	Intra-consortium transfer
        42	District birth
        43	Transfer in
        44	Transfer out
        45	Movement in
        46	Movement out
        47	Died
        48	Still birth
        49	Living out, treated in
        50	Living in, treated out

         */

    }

    private static RegistrationType convertRegistrationType(Integer obj) throws Exception {
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
