package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
import java.math.BigDecimal;
 
public class Patient extends AbstractPcrCsvWriter {
 
 
 
  public Patient(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Long id,
                    Long organisationId,
                    String nhsNumber,
                    Long nhsNumberVerificationConceptId,
                    Date dateOfBirth,
                    Date dateOfDeath,
                    Long genderConceptId,
                    Long usualPractitionerId,
                    Long careProviderId,
                    Long enteredByPractitionerId,
                    String title,
                    String firstName,
                    String middleNames,
                    String lastName,
                    String previousLastName,
                    Long homeAddressId,
                    Boolean isSpineSensitive,
char ethnicCode
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertLong(id),
                    convertLong(organisationId),
                    nhsNumber,
                    convertLong(nhsNumberVerificationConceptId),
                    convertDate(dateOfBirth),
                    convertDate(dateOfDeath),
                    convertLong(genderConceptId),
                    convertLong(usualPractitionerId),
                    convertLong(careProviderId),
                    convertLong(enteredByPractitionerId),
                    title,
                    firstName,
                    middleNames,
                    lastName,
                    previousLastName,
                    convertLong(homeAddressId),
                    convertBoolean(isSpineSensitive),
                    String.valueOf(ethnicCode)
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
                       "entered_by_practitioner_id",
                       "title",
                       "first_name",
                       "middle_names",
                       "last_name",
                       "previous_last_name",
                       "home_address_id",
                       "is_spine_sensitive",
                     "ethnic_code"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    String.class,
                    Long.class,
                    Long.class,
                    String.class,
                    Long.class,
                    Date.class,
                    Date.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    String.class,
                    String.class,
                    String.class,
                    String.class,
                    String.class,
                    Long.class,
                    Boolean.class,
                    char.class
    }; 
}
}
