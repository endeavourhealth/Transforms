package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
 
public class AppointmentAttendance extends AbstractPcrCsvWriter {
 
 
 
  public AppointmentAttendance(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Long appointmentSlotId,
                    Long patientId,
                    Long enteredByPractitionerId,
                    Date actualStartTime,
                    Date actualEndTime,
Long statusConceptId
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertLong(appointmentSlotId),
                    convertLong(patientId),
                    convertLong(enteredByPractitionerId),
                    convertDate(actualStartTime),
                    convertDate(actualEndTime),
                    convertLong(statusConceptId)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "appointment_slot_id",
                       "patient_id",
                       "entered_by_practitioner_id",
                       "actual_start_time",
                       "actual_end_time",
                     "status_concept_id"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    Long.class,
                    Long.class,
                    Long.class,
                    Date.class,
                    Date.class,
                    Long.class
    }; 
}
}
