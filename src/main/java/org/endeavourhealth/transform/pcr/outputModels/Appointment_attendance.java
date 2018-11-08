package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
 
public class Appointment_attendance extends AbstractPcrCsvWriter {
 
 
 
  public Appointment_attendance(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Integer appointment_slot_id,
                    Integer patient_id,
                    Date actual_start_time,
                    Date actual_end_time,
Long status_concept_id
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertInt(appointment_slot_id),
                    convertInt(patient_id),
                    convertDate(actual_start_time),
                    convertDate(actual_end_time),
convertLong(status_concept_id)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "appointment_slot_id",
                       "patient_id",
                       "actual_start_time",
                       "actual_end_time",
                     "status_concept_id"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    Integer.class,
                    Integer.class,
                    Date.class,
                    Date.class,
                    Long.class
    }; 
}
}
