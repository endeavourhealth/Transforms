package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
import java.math.BigDecimal;
 
public class AppointmentBooking extends AbstractPcrCsvWriter {
 
 
 
  public AppointmentBooking(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Long appointmentSlotId,
                    Date bookingTime,
                    Long patientId,
                    Long enteredByPractitionerId,
                    Long bookingConceptId,
String reason
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertLong(appointmentSlotId),
                    convertDate(bookingTime),
                    convertLong(patientId),
                    convertLong(enteredByPractitionerId),
                    convertLong(bookingConceptId),
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
                       "entered_by_practitioner_id",
                       "booking_concept_id",
                     "reason"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    String.class,
                    Long.class,
                    Date.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    String.class
    }; 
}
}
