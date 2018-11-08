package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
 
public class Related_person_relationship extends AbstractPcrCsvWriter {
 
 
 
  public Related_person_relationship(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Integer patient_id,
                    Long related_person_id,
Long type_concept_id
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertInt(patient_id),
                    convertLong(related_person_id),
convertLong(type_concept_id)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "patient_id",
                       "related_person_id",
                     "type_concept_id"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    Integer.class,
                    Long.class,
                    Long.class
    }; 
}
}
