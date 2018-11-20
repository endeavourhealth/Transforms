package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;

import java.util.Date;
 
public class AccidentEmergencyAttendance extends AbstractPcrCsvWriter {
 
 
 
  public AccidentEmergencyAttendance(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Long id,
                    Long patientId,
                    Long careEpisodeId,
                    Date arrivalDate,
                    Date effectiveDate,
                    Date triageStartDate,
                    Date triageEndDate,
                    Long effectivePractitionerId,
                    Long enteredByPractitionerId,
                    Long triagePractitionerId,
                    Date endDate,
                    Long receivedPractitionerId,
                    Long owningOrganisationId,
                    Long statusConceptId,
                    Boolean isConfidential,
                    Long reasonConceptId,
                    Long freeTextId,
                    String ambulanceNumber,
                    Long locationId,
Boolean isConsent
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertLong(id),
                    convertLong(patientId),
                    convertLong(careEpisodeId),
                    convertDate(arrivalDate),
                    convertDate(effectiveDate),
                    convertDate(triageStartDate),
                    convertDate(triageEndDate),
                    convertLong(effectivePractitionerId),
                    convertLong(enteredByPractitionerId),
                    convertLong(triagePractitionerId),
                    convertDate(endDate),
                    convertLong(receivedPractitionerId),
                    convertLong(owningOrganisationId),
                    convertLong(statusConceptId),
                    convertBoolean(isConfidential),
                    convertLong(reasonConceptId),
                    convertLong(freeTextId),
                    ambulanceNumber,
                    convertLong(locationId),
                    convertBoolean(isConsent)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "id",
                       "patient_id",
                       "care_episode_id",
                       "arrival_date",
                       "effective_date",
                       "triage_start_date",
                       "triage_end_date",
                       "effective_practitioner_id",
                       "entered_by_practitioner_id",
                       "triage_practitioner_id",
                       "end_date",
                       "received_practitioner_id",
                       "owning_organisation_id",
                       "status_concept_id",
                       "is_confidential",
                       "reason_concept_id",
                       "free_text_id",
                       "ambulance_number",
                       "location_id",
                     "is_consent"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    String.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    Date.class,
                    Date.class,
                    Date.class,
                    Date.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    Date.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    Boolean.class,
                    Long.class,
                    Long.class,
                    String.class,
                    Long.class,
                    Boolean.class
    }; 
}
}
