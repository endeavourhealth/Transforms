package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
 
public class Event_relationship extends AbstractPcrCsvWriter {
 
 
 
  public Event_relationship(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Long item_id,
                    Integer item_type,
                    Long linked_item_id,
Long linked_item_relationship_concept_id
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertLong(item_id),
                    convertInt(item_type),
                    convertLong(linked_item_id),
convertLong(linked_item_relationship_concept_id)
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
                    Long.class,
                    Integer.class,
                    Long.class,
                    Long.class
    }; 
}
}
