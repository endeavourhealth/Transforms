package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
 
public class Additional_relationship extends AbstractPcrCsvWriter {
 
 
 
  public Additional_relationship(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Integer id,
                    String source_table,
                    String related_table,
                    Long relationship_type_concept_id,
                    Long source_item_id,
Long related_item_id
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertInt(id),
                    source_table,
                    related_table,
                    convertLong(relationship_type_concept_id),
                    convertLong(source_item_id),
convertLong(related_item_id)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "id",
                       "source_table",
                       "related_table",
                       "relationship_type_concept_id",
                       "source_item_id",
                     "related_item_id"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    Integer.class,
                    String.class,
                    String.class,
                    Long.class,
                    Long.class,
                    Long.class
    }; 
}
}
