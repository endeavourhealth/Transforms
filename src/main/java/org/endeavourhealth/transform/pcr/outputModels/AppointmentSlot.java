package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
import java.math.BigDecimal;
 
public class AppointmentSlot extends AbstractPcrCsvWriter {
 
 
 
  public AppointmentSlot(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Long id,
                    Long appointmentScheduleId,
                    Date slotStart,
                    Date slotEnd,
                    Long plannedDurationMinutes,
                    Long typeConceptId,
Long interactionConceptId
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertLong(id),
                    convertLong(appointmentScheduleId),
                    convertDate(slotStart),
                    convertDate(slotEnd),
                    convertLong(plannedDurationMinutes),
                    convertLong(typeConceptId),
                    convertLong(interactionConceptId)
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
                    String.class,
                    Long.class,
                    Long.class,
                    Date.class,
                    Date.class,
                    Long.class,
                    Long.class,
                    Long.class
    }; 
}
}
