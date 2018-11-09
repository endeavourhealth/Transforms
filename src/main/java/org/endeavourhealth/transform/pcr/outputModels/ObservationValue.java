package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
 
public class ObservationValue extends AbstractPcrCsvWriter {
 
 
 
  public ObservationValue(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Integer patient_id,
                    Long observation_id,
                    Long operator_concept_id,
                    Double result_value,
                    String result_value_units,
                    Date result_date,
                    String result_text,
                    Long result_concept_id,
Long reference_range_id
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertInt(patient_id),
                    convertLong(observation_id),
                    convertLong(operator_concept_id),
                    convertDouble(result_value),
                    result_value_units,
                    convertDate(result_date),
                    result_text,
                    convertLong(result_concept_id),
convertLong(reference_range_id)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "patient_id",
                       "observation_id",
                       "operator_concept_id",
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
                    Integer.class,
                    Long.class,
                    Long.class,
                    Double.class,
                    String.class,
                    Date.class,
                    String.class,
                    Long.class,
                    Long.class
    }; 
}
}
