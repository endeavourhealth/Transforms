package org.endeavourhealth.transform.pcr;

import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.endeavourhealth.transform.pcr.outputModels.EventLog;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;

/**
 * Utility class for various generic methods
 */
public class FhirToPcrHelper {


    public static void freeTextWriter(long textId, long patientId, String freeText, long enteredByPractitionerId,
                                      AbstractPcrCsvWriter csvWriter) throws Exception {
//  Isn't a transformer in its own right. Called by transformers to add FreeText
        // May need to return id?
        // If we need a transformer for some data sources we can just write a wrapper for this method.
        // TODO check if it needs to return Id. Esp if we use same id map technique


        //    Long textId =  AbstractTransformer.findOrCreatePcrId(params, resourceType.toString(), resourceId.toString());

        org.endeavourhealth.transform.pcr.outputModels.FreeText model
                = (org.endeavourhealth.transform.pcr.outputModels.FreeText) csvWriter;
        model.writeUpsert(textId,
                patientId,
                enteredByPractitionerId,
                freeText);
    }


    public static void eventLogWriter(Long organisationId, Date entryDate, Long entryPractitionerId,
                                      Integer entryMode, String tableName, Long itemId,
                                      AbstractPcrCsvWriter csvWriter) throws Exception {

        EventLog model = (EventLog) csvWriter;
        model.writeUpsert(null, // Allow to auto increment
                organisationId,
                entryDate,
                entryPractitionerId,
                null, // not used yet
                entryMode,
                getTableIndex(tableName),
                itemId);
    }

    //TODO replace with a proper call to core dal layer
    private static int getTableIndex(String tableName) {
        int tabNumber = 0;
        switch (tableName) {
            case "accident_emergency_attendance":
                tabNumber = 1;
                break;
            case "additional_attribute":
                tabNumber = 2;
                break;
            case "additional_relationship":
                tabNumber = 3;
                break;
            case "address":
                tabNumber = 4;
                break;
            case "allergy":
                tabNumber = 5;
                break;
            case "appointment_attendance":
                tabNumber = 6;
                break;
            case "appointment_attendance_event":
                tabNumber = 7;
                break;
            case "appointment_booking":
                tabNumber = 8;
                break;
            case "appointment_schedule":
                tabNumber = 9;
                break;
            case "appointment_schedule_practitioner":
                tabNumber = 10;
                break;
            case "appointment_slot":
                tabNumber = 11;
                break;
            case "care_episode":
                tabNumber = 12;
                break;
            case "care_episode_additional_practitioner":
                tabNumber = 13;
                break;
            case "care_episode_status":
                tabNumber = 14;
                break;
            case "care_plan":
                tabNumber = 15;
                break;
            case "care_plan_activity":
                tabNumber = 16;
                break;
            case "care_plan_activity_target":
                tabNumber = 17;
                break;
            case "consultation":
                tabNumber = 18;
                break;
            case "data_entry_prompt":
                tabNumber = 19;
                break;
            case "device":
                tabNumber = 20;
                break;
            case "event_log":
                tabNumber = 21;
                break;
            case "event_relationship":
                tabNumber = 22;
                break;
            case "flag":
                tabNumber = 23;
                break;
            case "free_text":
                tabNumber = 24;
                break;
            case "gp_registration_status":
                tabNumber = 25;
                break;
            case "hospital_admission":
                tabNumber = 26;
                break;
            case "hospital_discharge":
                tabNumber = 27;
                break;
            case "hospital_ward_transfer":
                tabNumber = 28;
                break;
            case "immunisation":
                tabNumber = 29;
                break;
            case "location":
                tabNumber = 30;
                break;
            case "location_contact":
                tabNumber = 31;
                break;
            case "medication_amount":
                tabNumber = 32;
                break;
            case "medication_order":
                tabNumber = 33;
                break;
            case "medication_statement":
                tabNumber = 34;
                break;
            case "observation":
                tabNumber = 35;
                break;
            case "observation_value":
                tabNumber = 36;
                break;
            case "organisation":
                tabNumber = 37;
                break;
            case "patient":
                tabNumber = 38;
                break;
            case "patient_address":
                tabNumber = 39;
                break;
            case "patient_contact":
                tabNumber = 40;
                break;
            case "patient_identifier":
                tabNumber = 41;
                break;
            case "pcr_db_map":
                tabNumber = 42;
                break;
            case "pcr_id_map":
                tabNumber = 43;
                break;
            case "pcr_tables":
                tabNumber = 44;
                break;
            case "practitioner":
                tabNumber = 45;
                break;
            case "practitioner_contact":
                tabNumber = 46;
                break;
            case "practitioner_identifier":
                tabNumber = 47;
                break;
            case "problem":
                tabNumber = 48;
                break;
            case "procedure":
                tabNumber = 49;
                break;
            case "procedure_device":
                tabNumber = 50;
                break;
        }
        return tabNumber;
    }

    public static int getCodingScheme(String scheme) throws Exception {
        int ret = 99;
        //String system = getDomainName(scheme);

          if (scheme.equals("http://read.info/ctv2")) {
              ret = 0;
          } else if (scheme.equals("http://read.info/ctv3")) {
              ret = 1;
          }else if (scheme.equals("http://snomed.info/sct")) {
              ret = 2;
          } else if (scheme.contains("hl7.org")) {
                ret = 6;
        }
        //case ICD-10 =3
        // case OPCS-4 = 4
        //case millenium = 5
        return ret;
    }

    private static String getDomainName(String url) throws URISyntaxException {
        URI uri = new URI(url);
        return uri.getHost();
    }

}


