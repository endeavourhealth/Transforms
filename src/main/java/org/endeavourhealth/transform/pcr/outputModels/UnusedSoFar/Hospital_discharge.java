package org.endeavourhealth.transform.pcr.outputModels.UnusedSoFar;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.endeavourhealth.transform.pcr.outputModels.OutputContainer;

import java.util.Date;
 
public class Hospital_discharge extends AbstractPcrCsvWriter {
 
 
 
  public Hospital_discharge(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Long id,
                    Integer patient_id,
                    Integer care_episode_id,
                    Date effective_date,
                    Integer effective_date_precision,
                    Integer effective_practitioner_id,
                    Date end_date,
                    Integer owning_organisation_id,
                    Long status_concept_id,
                    Boolean is_confidential,
                    Long reason_concept_id,
                    Long free_text_id,
                    Integer location_id,
Boolean is_consent
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertLong(id),
                    convertInt(patient_id),
                    convertInt(care_episode_id),
                    convertDate(effective_date),
                    convertInt(effective_date_precision),
                    convertInt(effective_practitioner_id),
                    convertDate(end_date),
                    convertInt(owning_organisation_id),
                    convertLong(status_concept_id),
                    convertBoolean(is_confidential),
                    convertLong(reason_concept_id),
                    convertLong(free_text_id),
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
                       "effective_date",
                       "effective_date_precision",
                       "effective_practitioner_id",
                       "end_date",
                       "owning_organisation_id",
                       "status_concept_id",
                       "is_confidential",
                       "reason_concept_id",
                       "free_text_id",
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
                    Integer.class,
                    Integer.class,
                    Date.class,
                    Integer.class,
                    Long.class,
                    Boolean.class,
                    Long.class,
                    Long.class,
                    Integer.class,
                    Boolean.class
    }; 
}
}
