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
    public void writeUpsert(Long id,
                    Long organisationId,
                    String title,
                    String firstName,
                    String middleNames,
                    String lastName,
                    Long genderConceptId,
                    Date dateOfBirth,
                    Boolean isActive,
                    Long roleConceptId,
                    Long specialityConceptId,
Long enteredByPractitionerId
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertLong(id),
                    convertLong(organisationId),
                    title,
                    firstName,
                    middleNames,
                    lastName,
                    convertLong(genderConceptId),
                    convertDate(dateOfBirth),
                    convertBoolean(isActive),
                    convertLong(roleConceptId),
                    convertLong(specialityConceptId),
                    convertLong(enteredByPractitionerId)
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
                       "speciality_concept_id",
                     "entered_by_practitioner_id"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    Long.class,
                    Long.class,
                    String.class,
                    String.class,
                    String.class,
                    String.class,
                    Long.class,
                    Date.class,
                    Boolean.class,
                    Long.class,
                    Long.class,
                    Long.class
    }; 
}
}
