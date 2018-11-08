package org.endeavourhealth.transform.pcr.outputModels.UnusedSoFar;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.endeavourhealth.transform.pcr.outputModels.OutputContainer;

import java.util.Date;
 
public class Appointment_schedule extends AbstractPcrCsvWriter {
 
 
 
  public Appointment_schedule(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Integer id,
                    Integer organisation_id,
                    Integer location_id,
                    String description,
                    Long type_concept_id,
                    Long speciality_concept_id,
                    Date schedule_start,
Date schedule_end
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertInt(id),
                    convertInt(organisation_id),
                    convertInt(location_id),
                    description,
                    convertLong(type_concept_id),
                    convertLong(speciality_concept_id),
                    convertDate(schedule_start),
convertDate(schedule_end)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "id",
                       "organisation_id",
                       "location_id",
                       "description",
                       "type_concept_id",
                       "speciality_concept_id",
                       "schedule_start",
                     "schedule_end"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    Integer.class,
                    Integer.class,
                    Integer.class,
                    String.class,
                    Long.class,
                    Long.class,
                    Date.class,
                    Date.class
    }; 
}
}
