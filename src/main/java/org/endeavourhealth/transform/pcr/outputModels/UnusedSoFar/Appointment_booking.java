package org.endeavourhealth.transform.pcr.outputModels.UnusedSoFar;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.endeavourhealth.transform.pcr.outputModels.OutputContainer;

import java.util.Date;
 
public class Appointment_booking extends AbstractPcrCsvWriter {
 
 
 
  public Appointment_booking(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Integer appointment_slot_id,
                    Date booking_time,
                    Integer patient_id,
                    Long booking_concept_id,
String reason
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertInt(appointment_slot_id),
                    convertDate(booking_time),
                    convertInt(patient_id),
                    convertLong(booking_concept_id),
reason
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "appointment_slot_id",
                       "booking_time",
                       "patient_id",
                       "booking_concept_id",
                     "reason"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    Integer.class,
                    Date.class,
                    Integer.class,
                    Long.class,
                    String.class
    }; 
}
}
