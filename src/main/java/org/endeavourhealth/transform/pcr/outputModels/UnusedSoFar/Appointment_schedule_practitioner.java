package org.endeavourhealth.transform.pcr.outputModels.UnusedSoFar;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.endeavourhealth.transform.pcr.outputModels.OutputContainer;

import java.util.Date;
 
public class Appointment_schedule_practitioner extends AbstractPcrCsvWriter {
 
 
 
  public Appointment_schedule_practitioner(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Integer appointment_schedule_id,
                    Integer practitioner_id,
Boolean is_main_practitioner
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertInt(appointment_schedule_id),
                    convertInt(practitioner_id),
convertBoolean(is_main_practitioner)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "appointment_schedule_id",
                       "practitioner_id",
                     "is_main_practitioner"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    Integer.class,
                    Integer.class,
                    Boolean.class
    }; 
}
}
