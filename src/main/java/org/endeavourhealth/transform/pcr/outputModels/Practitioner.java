package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
 
public class Practitioner extends AbstractPcrCsvWriter {
 
 
 
  public Practitioner(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Integer id,
                    Integer organisation_id,
                    String title,
                    String first_name,
                    String middle_names,
                    String last_name,
                    Long gender_concept_id,
                    Date date_of_birth,
                    Boolean is_active,
                    Long role_concept_id,
Long speciality_concept_id
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertInt(id),
                    convertInt(organisation_id),
                    title,
                    first_name,
                    middle_names,
                    last_name,
                    convertLong(gender_concept_id),
                    convertDate(date_of_birth),
                    convertBoolean(is_active),
                    convertLong(role_concept_id),
convertLong(speciality_concept_id)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "id",
                       "organisation_id",
                       "title",
                       "first_name",
                       "middle_names",
                       "last_name",
                       "gender_concept_id",
                       "date_of_birth",
                       "is_active",
                       "role_concept_id",
                     "speciality_concept_id"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    Integer.class,
                    Integer.class,
                    String.class,
                    String.class,
                    String.class,
                    String.class,
                    Long.class,
                    Date.class,
                    Boolean.class,
                    Long.class,
                    Long.class
    }; 
}
}
