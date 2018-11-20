package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
import java.math.BigDecimal;
 
public class EventRelationship extends AbstractPcrCsvWriter {
 
 
 
  public EventRelationship(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Long itemId,
                    Integer itemType,
                    Long linkedItemId,
Long linkedItemRelationshipConceptId
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertLong(itemId),
                    convertInt(itemType),
                    convertLong(linkedItemId),
                    convertLong(linkedItemRelationshipConceptId)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "item_id",
                       "item_type",
                       "linked_item_id",
                     "linked_item_relationship_concept_id"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    String.class,
                    Long.class,
                    Integer.class,
                    Long.class,
                    Long.class
    }; 
}
}
