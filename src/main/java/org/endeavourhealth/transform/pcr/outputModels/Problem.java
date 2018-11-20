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
                    Long patientId,
                    Long observationId,
                    Long typeConceptId,
                    Long significanceConceptId,
                    Integer expectedDurationDays,
                    Date lastReviewDate,
                    Long lastReviewPractitionerId,
Long enteredByPractitionerId
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertLong(id),
                    convertLong(patientId),
                    convertLong(observationId),
                    convertLong(typeConceptId),
                    convertLong(significanceConceptId),
                    convertInt(expectedDurationDays),
                    convertDate(lastReviewDate),
                    convertLong(lastReviewPractitionerId),
                    convertLong(enteredByPractitionerId)
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
                       "last_review_practitioner_id",
                     "entered_by_practitioner_id"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    String.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    Integer.class,
                    Date.class,
                    Long.class,
                    Long.class
    }; 
}
}
