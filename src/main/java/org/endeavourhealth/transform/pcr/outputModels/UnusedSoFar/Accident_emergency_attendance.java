package org.endeavourhealth.transform.pcr.outputModels.UnusedSoFar;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.endeavourhealth.transform.pcr.outputModels.OutputContainer;

import java.util.Date;
 
public class Accident_emergency_attendance extends AbstractPcrCsvWriter {
 
 
 
  public Accident_emergency_attendance(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Long id,
                    Integer patient_id,
                    Integer care_episode_id,
                    Date arrival_date,
                    Date effective_date,
                    Date triage_start_date,
                    Date triage_end_date,
                    Integer effective_practitioner_id,
                    Integer triage_practitioner_id,
                    Date end_date,
                    Integer received_practitioner_id,
                    Integer owning_organisation_id,
                    Long status_concept_id,
                    Boolean is_confidential,
                    Long reason_concept_id,
                    Long free_text_id,
                    String ambulance_number,
                    Integer location_id,
Boolean is_consent
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertLong(id),
                    convertInt(patient_id),
                    convertInt(care_episode_id),
                    convertDate(arrival_date),
                    convertDate(effective_date),
                    convertDate(triage_start_date),
                    convertDate(triage_end_date),
                    convertInt(effective_practitioner_id),
                    convertInt(triage_practitioner_id),
                    convertDate(end_date),
                    convertInt(received_practitioner_id),
                    convertInt(owning_organisation_id),
                    convertLong(status_concept_id),
                    convertBoolean(is_confidential),
                    convertLong(reason_concept_id),
                    convertLong(free_text_id),
                    ambulance_number,
                    convertInt(location_id),
convertBoolean(is_consent)
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
                    Long.class,
                    Integer.class,
                    Integer.class,
                    Date.class,
                    Date.class,
                    Date.class,
                    Date.class,
                    Integer.class,
                    Integer.class,
                    Date.class,
                    Integer.class,
                    Integer.class,
                    Long.class,
                    Boolean.class,
                    Long.class,
                    Long.class,
                    String.class,
                    Integer.class,
                    Boolean.class
    }; 
}
}
