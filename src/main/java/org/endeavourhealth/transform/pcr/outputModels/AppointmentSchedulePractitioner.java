package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
 
public class AppointmentSchedulePractitioner extends AbstractPcrCsvWriter {
 
 
 
  public AppointmentSchedulePractitioner(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Long appointmentScheduleId,
                    Long practitionerId,
                    Long enteredByPractitionerId,
Boolean isMainPractitioner
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertLong(appointmentScheduleId),
                    convertLong(practitionerId),
                    convertLong(enteredByPractitionerId),
                    convertBoolean(isMainPractitioner)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "appointment_schedule_id",
                       "practitioner_id",
                       "entered_by_practitioner_id",
                     "is_main_practitioner"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    Long.class,
                    Long.class,
                    Long.class,
                    Boolean.class
    }; 
}
}
