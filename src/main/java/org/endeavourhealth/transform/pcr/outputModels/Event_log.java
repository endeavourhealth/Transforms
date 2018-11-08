package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
 
public class Event_log extends AbstractPcrCsvWriter {
 
 
 
  public Event_log(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Long id,
                    Integer organisation_id,
                    Date entry_date,
                    Integer entry_practitioner_id,
                    Integer device_id,
                    Integer entry_mode,
                    String table_name,
Long item_id
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertLong(id),
                    convertInt(organisation_id),
                    convertDate(entry_date),
                    convertInt(entry_practitioner_id),
                    convertInt(device_id),
                    convertInt(entry_mode),
                    table_name,
convertLong(item_id)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "id",
                       "organisation_id",
                       "entry_date",
                       "entry_practitioner_id",
                       "device_id",
                       "entry_mode",
                       "table_name",
                     "item_id"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    Long.class,
                    Integer.class,
                    Date.class,
                    Integer.class,
                    Integer.class,
                    Integer.class,
                    String.class,
                    Long.class
    }; 
}
}
