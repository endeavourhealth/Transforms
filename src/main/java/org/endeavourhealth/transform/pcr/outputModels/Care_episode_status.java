package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
 
public class Care_episode_status extends AbstractPcrCsvWriter {
 
 
 
  public Care_episode_status(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Integer patient_id,
                    Integer owning_organisation_id,
                    Integer care_episode_id,
                    Date start_time,
                    Date end_time,
Long care_episode_status_concept_id
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertInt(patient_id),
                    convertInt(owning_organisation_id),
                    convertInt(care_episode_id),
                    convertDate(start_time),
                    convertDate(end_time),
convertLong(care_episode_status_concept_id)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "patient_id",
                       "owning_organisation_id",
                       "care_episode_id",
                       "start_time",
                       "end_time",
                     "care_episode_status_concept_id"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    Integer.class,
                    Integer.class,
                    Integer.class,
                    Date.class,
                    Date.class,
                    Long.class
    }; 
}
}
