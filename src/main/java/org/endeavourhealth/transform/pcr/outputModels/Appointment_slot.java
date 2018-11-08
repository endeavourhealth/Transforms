package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
 
public class Appointment_slot extends AbstractPcrCsvWriter {
 
 
 
  public Appointment_slot(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Integer id,
                    Integer appointment_schedule_id,
                    Date slot_start,
                    Date slot_end,
                    Integer planned_duration_minutes,
                    Long type_concept_id,
Long interaction_concept_id
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertInt(id),
                    convertInt(appointment_schedule_id),
                    convertDate(slot_start),
                    convertDate(slot_end),
                    convertInt(planned_duration_minutes),
                    convertLong(type_concept_id),
convertLong(interaction_concept_id)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "id",
                       "appointment_schedule_id",
                       "slot_start",
                       "slot_end",
                       "planned_duration_minutes",
                       "type_concept_id",
                     "interaction_concept_id"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    Integer.class,
                    Integer.class,
                    Date.class,
                    Date.class,
                    Integer.class,
                    Long.class,
                    Long.class
    }; 
}
}
