package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
 
public class EventLog extends AbstractPcrCsvWriter {
 
 
 
  public EventLog(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Long id,
                    Long organisationId,
                    Date entryDate,
                    Long enteredByPractitionerId,
                    Long deviceId,
                    Integer entryMode,
                    Integer tableId,
Long itemId
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertLong(id),
                    convertLong(organisationId),
                    convertDate(entryDate),
                    convertLong(enteredByPractitionerId),
                    convertLong(deviceId),
                    convertInt(entryMode),
                    convertInt(tableId),
                    convertLong(itemId)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "id",
                       "organisation_id",
                       "entry_date",
                       "entered_by_practitioner_id",
                       "device_id",
                       "entry_mode",
                       "table_id",
                     "item_id"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    Long.class,
                    Long.class,
                    Date.class,
                    Long.class,
                    Long.class,
                    Integer.class,
                    Integer.class,
                    Long.class
    }; 
}
}
