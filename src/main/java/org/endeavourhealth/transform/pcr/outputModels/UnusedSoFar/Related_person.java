package org.endeavourhealth.transform.pcr.outputModels.UnusedSoFar;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.endeavourhealth.transform.pcr.outputModels.OutputContainer;

import java.util.Date;
 
public class Related_person extends AbstractPcrCsvWriter {
 
 
 
  public Related_person(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Long id,
                    Integer patient_id,
                    String title,
                    String first_name,
                    String middle_names,
                    String last_name,
                    Date date_of_birth,
                    Boolean is_active,
                    Long type_concept_id,
                    Integer address_id,
                    Date start_date,
Date end_date
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertLong(id),
                    convertInt(patient_id),
                    title,
                    first_name,
                    middle_names,
                    last_name,
                    convertDate(date_of_birth),
                    convertBoolean(is_active),
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
                       "title",
                       "first_name",
                       "middle_names",
                       "last_name",
                       "date_of_birth",
                       "is_active",
                       "type_concept_id",
                       "address_id",
                       "start_date",
                     "end_date"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    Long.class,
                    Integer.class,
                    String.class,
                    String.class,
                    String.class,
                    String.class,
                    Date.class,
                    Boolean.class,
                    Long.class,
                    Integer.class,
                    Date.class,
                    Date.class
    }; 
}
}
