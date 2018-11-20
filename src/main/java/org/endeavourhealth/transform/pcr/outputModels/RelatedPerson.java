package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
import java.math.BigDecimal;
 
public class RelatedPerson extends AbstractPcrCsvWriter {
 
 
 
  public RelatedPerson(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Long id,
                    Long patientId,
                    String title,
                    String firstName,
                    String middleNames,
                    String lastName,
                    Date dateOfBirth,
                    Boolean isActive,
                    Long typeConceptId,
                    Long addressId,
                    Date startDate,
                    Date endDate,
Long enteredByPractitionerId
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertLong(id),
                    convertLong(patientId),
                    title,
                    firstName,
                    middleNames,
                    lastName,
                    convertDate(dateOfBirth),
                    convertBoolean(isActive),
                    convertLong(typeConceptId),
                    convertLong(addressId),
                    convertDate(startDate),
                    convertDate(endDate),
                    convertLong(enteredByPractitionerId)
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
                       "end_date",
                     "entered_by_practitioner_id"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    String.class,
                    Long.class,
                    Long.class,
                    String.class,
                    String.class,
                    String.class,
                    String.class,
                    Date.class,
                    Boolean.class,
                    Long.class,
                    Long.class,
                    Date.class,
                    Date.class,
                    Long.class
    }; 
}
}
