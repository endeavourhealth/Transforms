package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
 
public class PatientAddress extends AbstractPcrCsvWriter {
 
 
 
  public PatientAddress(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Long id,
                    Long patientId,
                    Long typeConceptId,
                    Long addressId,
                    Date startDate,
                    Date endDate,
Long enteredByPractitionerId
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertLong(id),
                    convertLong(patientId),
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
                    Long.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    Date.class,
                    Date.class,
                    Long.class
    }; 
}
}
