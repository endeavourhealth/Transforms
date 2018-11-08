package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
 
public class Location extends AbstractPcrCsvWriter {
 
 
 
  public Location(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Integer id,
                    Integer organisation_id,
                    String name,
                    Long type_concept_id,
                    Integer address_id,
                    Date start_date,
                    Date end_date,
                    Boolean is_active,
Integer parent_location_id
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertInt(id),
                    convertInt(organisation_id),
                    name,
                    convertLong(type_concept_id),
                    convertInt(address_id),
                    convertDate(start_date),
                    convertDate(end_date),
                    convertBoolean(is_active),
convertInt(parent_location_id)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "id",
                       "organisation_id",
                       "name",
                       "type_concept_id",
                       "address_id",
                       "start_date",
                       "end_date",
                       "is_active",
                     "parent_location_id"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    Integer.class,
                    Integer.class,
                    String.class,
                    Long.class,
                    Integer.class,
                    Date.class,
                    Date.class,
                    Boolean.class,
                    Integer.class
    }; 
}
}
