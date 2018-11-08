package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
 
public class Organisation extends AbstractPcrCsvWriter {
 
 
 
  public Organisation(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Integer id,
                    Integer service_id,
                    Integer system_id,
                    String ods_code,
                    String name,
                    Boolean is_active,
                    Integer parent_organisation_id,
                    Long type_concept_id,
Integer main_location_id
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertInt(id),
                    convertInt(service_id),
                    convertInt(system_id),
                    ods_code,
                    name,
                    convertBoolean(is_active),
                    convertInt(parent_organisation_id),
                    convertLong(type_concept_id),
convertInt(main_location_id)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "id",
                       "service_id",
                       "system_id",
                       "ods_code",
                       "name",
                       "is_active",
                       "parent_organisation_id",
                       "type_concept_id",
                     "main_location_id"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    Integer.class,
                    Integer.class,
                    Integer.class,
                    String.class,
                    String.class,
                    Boolean.class,
                    Integer.class,
                    Long.class,
                    Integer.class
    }; 
}
}
