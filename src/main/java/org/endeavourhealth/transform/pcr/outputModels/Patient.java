package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
 
public class Patient extends AbstractPcrCsvWriter {
 
 
 
  public Patient(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Integer id,
                    Integer organisation_id,
                    String nhs_number,
                    Long nhs_number_verification_concept_id,
                    Date date_of_birth,
                    Date date_of_death,
                    Long gender_concept_id,
                    Integer usual_practitioner_id,
                    Integer care_provider_id,
                    String title,
                    String first_name,
                    String middle_names,
                    String last_name,
                    String previous_last_name,
                    Integer home_address_id,
Boolean is_spine_sensitive
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertInt(id),
                    convertInt(organisation_id),
                    nhs_number,
                    convertLong(nhs_number_verification_concept_id),
                    convertDate(date_of_birth),
                    convertDate(date_of_death),
                    convertLong(gender_concept_id),
                    convertInt(usual_practitioner_id),
                    convertInt(care_provider_id),
                    title,
                    first_name,
                    middle_names,
                    last_name,
                    previous_last_name,
                    convertInt(home_address_id),
convertBoolean(is_spine_sensitive)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "id",
                       "organisation_id",
                       "nhs_number",
                       "nhs_number_verification_concept_id",
                       "date_of_birth",
                       "date_of_death",
                       "gender_concept_id",
                       "usual_practitioner_id",
                       "care_provider_id",
                       "title",
                       "first_name",
                       "middle_names",
                       "last_name",
                       "previous_last_name",
                       "home_address_id",
                     "is_spine_sensitive"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    Integer.class,
                    Integer.class,
                    String.class,
                    Long.class,
                    Date.class,
                    Date.class,
                    Long.class,
                    Integer.class,
                    Integer.class,
                    String.class,
                    String.class,
                    String.class,
                    String.class,
                    String.class,
                    Integer.class,
                    Boolean.class
    }; 
}
}
