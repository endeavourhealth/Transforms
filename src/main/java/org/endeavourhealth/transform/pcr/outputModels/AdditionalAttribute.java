package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;

import java.math.BigDecimal;
import java.util.Date;
 
public class AdditionalAttribute extends AbstractPcrCsvWriter {
 
 
 
  public AdditionalAttribute(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Long patientId,
                    Integer itemType,
                    Long itemId,
                    Long conceptId,
                    BigDecimal attributeValue,
                    Date attributeDate,
                    String attributeText,
                    Long attributeTextConceptId,
Boolean isConsent
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertLong(patientId),
                    convertInt(itemType),
                    convertLong(itemId),
                    convertLong(conceptId),
                    convertBigDecimal(attributeValue),
                    convertDate(attributeDate),
                    attributeText,
                    convertLong(attributeTextConceptId),
                    convertBoolean(isConsent)
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
                    Long.class,
                    Integer.class,
                    Long.class,
                    Long.class,
                    BigDecimal.class,
                    Date.class,
                    String.class,
                    Long.class,
                    Boolean.class
    }; 
}
}
