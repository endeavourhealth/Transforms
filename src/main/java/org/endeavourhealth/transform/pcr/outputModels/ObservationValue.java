package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;

import java.math.BigDecimal;
import java.util.Date;
 
public class ObservationValue extends AbstractPcrCsvWriter {
 
 
 
  public ObservationValue(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Long patientId,
                    Long observationId,
                    Long operatorConceptId,
                    Long enteredByPractitionerId,
                    BigDecimal resultValue,
                    String resultValueUnits,
                    Date resultDate,
                    String resultText,
                    Long resultConceptId,
Long referenceRangeId
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertLong(patientId),
                    convertLong(observationId),
                    convertLong(operatorConceptId),
                    convertLong(enteredByPractitionerId),
                    convertBigDecimal(resultValue),
                    resultValueUnits,
                    convertDate(resultDate),
                    resultText,
                    convertLong(resultConceptId),
                    convertLong(referenceRangeId)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "patient_id",
                       "observation_id",
                       "operator_concept_id",
                       "entered_by_practitioner_id",
                       "result_value",
                       "result_value_units",
                       "result_date",
                       "result_text",
                       "result_concept_id",
                     "reference_range_id"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    Long.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    BigDecimal.class,
                    String.class,
                    Date.class,
                    String.class,
                    Long.class,
                    Long.class
    }; 
}
}
