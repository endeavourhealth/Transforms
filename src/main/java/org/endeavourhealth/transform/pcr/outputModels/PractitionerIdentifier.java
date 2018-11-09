package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
 
public class PractitionerIdentifier extends AbstractPcrCsvWriter {
 
 
 
  public PractitionerIdentifier(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Integer practitioner_id,
                    Long type_concept_id,
String value
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertInt(practitioner_id),
                    convertLong(type_concept_id),
value
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "practitioner_id",
                       "type_concept_id",
                     "value"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    Integer.class,
                    Long.class,
                    String.class
    }; 
}
}
