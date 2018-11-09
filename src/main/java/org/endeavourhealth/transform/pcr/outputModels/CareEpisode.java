package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
 
public class CareEpisode extends AbstractPcrCsvWriter {
 
 
 
  public CareEpisode(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Integer id,
                    Integer patient_id,
                    Integer owning_organisation_id,
                    Date effective_date,
                    Integer effective_date_precision,
                    Integer effective_practitioner_id,
                    Date end_date,
                    String encounter_link_id,
                    Long status_concept_id,
                    Long speciality_concept_id,
                    Long admin_concept_id,
                    Long reason_concept_id,
                    Long type_concept_id,
                    Integer location_id,
                    Integer referral_request_id,
                    Boolean is_consent,
Integer latest_care_episode_status_id
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertInt(id),
                    convertInt(patient_id),
                    convertInt(owning_organisation_id),
                    convertDate(effective_date),
                    convertInt(effective_date_precision),
                    convertInt(effective_practitioner_id),
                    convertDate(end_date),
                    encounter_link_id,
                    convertLong(status_concept_id),
                    convertLong(speciality_concept_id),
                    convertLong(admin_concept_id),
                    convertLong(reason_concept_id),
                    convertLong(type_concept_id),
                    convertInt(location_id),
                    convertInt(referral_request_id),
                    convertBoolean(is_consent),
convertInt(latest_care_episode_status_id)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "id",
                       "patient_id",
                       "owning_organisation_id",
                       "effective_date",
                       "effective_date_precision",
                       "effective_practitioner_id",
                       "end_date",
                       "encounter_link_id",
                       "status_concept_id",
                       "speciality_concept_id",
                       "admin_concept_id",
                       "reason_concept_id",
                       "type_concept_id",
                       "location_id",
                       "referral_request_id",
                       "is_consent",
                     "latest_care_episode_status_id"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    Integer.class,
                    Integer.class,
                    Integer.class,
                    Date.class,
                    Integer.class,
                    Integer.class,
                    Date.class,
                    String.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    Integer.class,
                    Integer.class,
                    Boolean.class,
                    Integer.class
    }; 
}
}
