package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
 
public class Patient_address extends AbstractPcrCsvWriter {
 
 
 
  public Patient_address(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Integer id,
                    Integer patient_id,
                    Long type_concept_id,
                    Integer address_id,
                    Date start_date,
Date end_date
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertInt(id),
                    convertInt(patient_id),
                    convertLong(type_concept_id),
                    convertInt(address_id),
                    convertDate(start_date),
convertDate(end_date)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "id",
                       "patient_id",
                       "type_concept_id",
                       "address_id",
                       "start_date",
                     "end_date"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    Integer.class,
                    Integer.class,
                    Long.class,
                    Integer.class,
                    Date.class,
                    Date.class
    }; 
}
}
