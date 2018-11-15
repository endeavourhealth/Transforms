package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
 
public class RelatedPersonContact extends AbstractPcrCsvWriter {
 
 
 
  public RelatedPersonContact(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Long patientId,
                    Long relatedPersonId,
                    Long typeConceptId,
                    String value,
Long enteredByPractitionerId
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertLong(patientId),
                    convertLong(relatedPersonId),
                    convertLong(typeConceptId),
                    value,
                    convertLong(enteredByPractitionerId)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "patient_id",
                       "related_person_id",
                       "type_concept_id",
                       "value",
                     "entered_by_practitioner_id"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    Long.class,
                    Long.class,
                    Long.class,
                    String.class,
                    Long.class
    }; 
}
}
