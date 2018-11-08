package org.endeavourhealth.transform.pcr.outputModels.UnusedSoFar;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.endeavourhealth.transform.pcr.outputModels.OutputContainer;

import java.util.Date;
 
public class Additional_attribute extends AbstractPcrCsvWriter {
 
 
 
  public Additional_attribute(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Integer patient_id,
                    Integer item_type,
                    Long item_id,
                    Long concept_id,
                    Double attribute_value,
                    Date attribute_date,
                    String attribute_text,
                    Long attribute_text_concept_id,
Boolean is_consent
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertInt(patient_id),
                    convertInt(item_type),
                    convertLong(item_id),
                    convertLong(concept_id),
                    convertDouble(attribute_value),
                    convertDate(attribute_date),
                    attribute_text,
                    convertLong(attribute_text_concept_id),
convertBoolean(is_consent)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "patient_id",
                       "item_type",
                       "item_id",
                       "concept_id",
                       "attribute_value",
                       "attribute_date",
                       "attribute_text",
                       "attribute_text_concept_id",
                     "is_consent"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    Integer.class,
                    Integer.class,
                    Long.class,
                    Long.class,
                    Double.class,
                    Date.class,
                    String.class,
                    Long.class,
                    Boolean.class
    }; 
}
}
