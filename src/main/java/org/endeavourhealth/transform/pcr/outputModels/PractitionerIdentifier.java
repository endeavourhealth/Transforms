package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
import java.math.BigDecimal;
 
public class PractitionerIdentifier extends AbstractPcrCsvWriter {
 
 
 
  public PractitionerIdentifier(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Long practitionerId,
                    Long typeConceptId,
                    String value,
Long enteredByPractitionerId
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertLong(practitionerId),
                    convertLong(typeConceptId),
                    value,
                    convertLong(enteredByPractitionerId)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "practitioner_id",
                       "type_concept_id",
                       "value",
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
                    Long.class
    }; 
}
}