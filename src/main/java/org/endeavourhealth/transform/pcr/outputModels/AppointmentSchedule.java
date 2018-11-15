package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
 
public class AppointmentSchedule extends AbstractPcrCsvWriter {
 
 
 
  public AppointmentSchedule(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Long id,
                    Long organisationId,
                    Long locationId,
                    String description,
                    Long typeConceptId,
                    Long specialityConceptId,
                    Date scheduleStart,
Date scheduleEnd
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertLong(id),
                    convertLong(organisationId),
                    convertLong(locationId),
                    description,
                    convertLong(typeConceptId),
                    convertLong(specialityConceptId),
                    convertDate(scheduleStart),
                    convertDate(scheduleEnd)
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
                    Long.class,
                    Long.class,
                    Long.class,
                    String.class,
                    Long.class,
                    Long.class,
                    Date.class,
                    Date.class
    }; 
}
}
