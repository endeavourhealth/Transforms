package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
 
public class Problem extends AbstractPcrCsvWriter {
 
 
 
  public Problem(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Long id,
                    Integer patient_id,
                    Long observation_id,
                    Long type_concept_id,
                    Long significance_concept_id,
                    Integer expected_duration_days,
                    Date last_review_date,
Integer last_review_practitioner_id
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertLong(id),
                    convertInt(patient_id),
                    convertLong(observation_id),
                    convertLong(type_concept_id),
                    convertLong(significance_concept_id),
                    convertInt(expected_duration_days),
                    convertDate(last_review_date),
convertInt(last_review_practitioner_id)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "id",
                       "patient_id",
                       "observation_id",
                       "type_concept_id",
                       "significance_concept_id",
                       "expected_duration_days",
                       "last_review_date",
                     "last_review_practitioner_id"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    Long.class,
                    Integer.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    Integer.class,
                    Date.class,
                    Integer.class
    }; 
}
}
