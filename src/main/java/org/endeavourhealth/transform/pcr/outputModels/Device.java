package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
 
public class Device extends AbstractPcrCsvWriter {
 
 
 
  public Device(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Integer id,
                    Integer organisation_id,
                    Long type_concept_id,
                    String serial_number,
                    String device_name,
                    String manufacturer,
                    String human_readable_identifier,
                    byte[] mahine_readable_identifier,
String version
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertInt(id),
                    convertInt(organisation_id),
                    convertLong(type_concept_id),
                    serial_number,
                    device_name,
                    manufacturer,
                    human_readable_identifier,
                    convertBytes(mahine_readable_identifier),
version
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "id",
                       "organisation_id",
                       "type_concept_id",
                       "serial_number",
                       "device_name",
                       "manufacturer",
                       "human_readable_identifier",
                       "mahine_readable_identifier",
                     "version"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    Integer.class,
                    Integer.class,
                    Long.class,
                    String.class,
                    String.class,
                    String.class,
                    String.class,
                    byte[].class,
                    String.class
    }; 
}
}
