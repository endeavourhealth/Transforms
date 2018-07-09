package org.endeavourhealth.transform.emis.csv.transforms.bespoke;

import org.endeavourhealth.common.fhir.schema.RegistrationType;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.schema.appointment.Session;
import org.endeavourhealth.transform.emis.csv.schema.bespoke.RegistrationStatus;
import org.hl7.fhir.instance.model.EpisodeOfCare;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.Map;

public class RegistrationStatusTransformer {

    public static void transform(String version,
                                 Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(Session.class);
        while (parser.nextRecord()) {

            try {
                processRecord((RegistrationStatus)parser, fhirResourceFiler, csvHelper);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    private static void processRecord(RegistrationStatus parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       EmisCsvHelper csvHelper) throws Exception {

        CsvCell patientGuidCell = parser.getPatientGuid();
        String patientGuid = patientGuidCell.getString();

        //the patient GUID in the standard extract files is in upper case and
        //has curly braces around it, so we need to ensure this is the same
        patientGuid = "{" + patientGuid.toUpperCase() + "}";

        EpisodeOfCare episodeOfCare = (EpisodeOfCare)csvHelper.retrieveResource(patientGuid, ResourceType.EpisodeOfCare);
        EpisodeOfCareBuilder episodeBuilder = new EpisodeOfCareBuilder(episodeOfCare);

        //don't carry over the registration type from this file, since we get this in the normal daily extract, which is more up to date
        /*CsvCell registrationTypeIdCell = parser.getRegistrationTypeId();
        RegistrationType registrationType = convertRegistrationType(registrationTypeIdCell.getInt());
        episodeBuilder.setRegistrationType(registrationType, registrationTypeIdCell);*/

        CsvCell registrationStatusIdCell = parser.getRegistrationStatusId();
        String registrationStatus = convertRegistrationStatus(registrationStatusIdCell.getInt());
        episodeBuilder.setMedicalRecordStatus(registrationStatus, registrationStatusIdCell);

        //TODO - should we keep the full registration status on the episode???
        //TODO - should we carry over the concept of a registration status being "active" or not? Status 1-3 are NOT counted as active?
        //TODO - how to handle multiple records per patient? Make this like a pre-transformer or something?

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), false, episodeBuilder);
    }

    private static String convertRegistrationStatus(Integer obj) {
        int value = obj.intValue();

        //TODO

        return null;
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

        } else {
            throw new TransformException("Unsupported registration type " + value);
        }

        //known types that aren't supported below:
        /*9	Contraceptive Services
        10	Maternity Services
        11	Child Health Services
        12	Walk-In Patient
        13	Minor Surgery
        14	Sexual Health
        15	Pre Registration
        16	Yellow Fever
        17	Dermatology
        18	Diabetic
        19	Rheumatology
        20	Chiropody
        21	Coronary Health Checks
        22	Ultrasound
        23	BCG Clinic
        24	Vasectomy
        25	Acupuncture
        26	Reflexology
        27	Hypnotherapy
        28	Out of Hours
        29	Rehabilitation
        30	Antenatal
        31	Audiology
        32	Gynaecology
        33	Doppler
        34	Secondary Registration
        35	Urgent and Emergency Care
        36	Externally Registered*/

    }
}
