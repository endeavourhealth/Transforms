package org.endeavourhealth.transform.pcr.outputModels.UnusedSoFar;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.endeavourhealth.transform.pcr.outputModels.OutputContainer;

import java.util.Date;
 
public class Appointment_attendance_event extends AbstractPcrCsvWriter {
 
 
 
  public Appointment_attendance_event(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Integer appointment_slot_id,
                    Date event_time,
Long event_concept_id
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertInt(appointment_slot_id),
                    convertDate(event_time),
convertLong(event_concept_id)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "appointment_slot_id",
                       "event_time",
                     "event_concept_id"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    Integer.class,
                    Date.class,
                    Long.class
    }; 
}
}
