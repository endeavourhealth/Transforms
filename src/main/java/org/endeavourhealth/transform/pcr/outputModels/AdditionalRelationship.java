package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
 
public class AdditionalRelationship extends AbstractPcrCsvWriter {
 
 
 
  public AdditionalRelationship(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Long id,
                    String sourceTable,
                    String relatedTable,
                    Long relationshipTypeConceptId,
                    Long sourceItemId,
Long relatedItemId
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertLong(id),
                    sourceTable,
                    relatedTable,
                    convertLong(relationshipTypeConceptId),
                    convertLong(sourceItemId),
                    convertLong(relatedItemId)
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
                    Long.class,
                    String.class,
                    String.class,
                    Long.class,
                    Long.class,
                    Long.class
    }; 
}
}
