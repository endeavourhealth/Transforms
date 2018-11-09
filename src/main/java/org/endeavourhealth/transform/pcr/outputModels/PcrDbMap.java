package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
 
public class PcrDbMap extends AbstractPcrCsvWriter {
 
 
 
  public PcrDbMap(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Integer id,
                    String discoveryDb,
String discoverySchema
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertInt(id),
                    discoveryDb,
discoverySchema
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "id",
                       "discoveryDb",
                     "discoverySchema"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    Integer.class,
                    String.class,
                    String.class
    }; 
}
}
