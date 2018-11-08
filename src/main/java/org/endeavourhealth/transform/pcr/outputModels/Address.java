package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
 
public class Address extends AbstractPcrCsvWriter {
 
 
 
  public Address(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Integer id,
                    String address_line_1,
                    String address_line_2,
                    String address_line_3,
                    String address_line_4,
                    String postcode,
                    Long uprn,
                    Long approximation_concept_id,
Long property_type_concept_id
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertInt(id),
                    address_line_1,
                    address_line_2,
                    address_line_3,
                    address_line_4,
                    postcode,
                    convertLong(uprn),
                    convertLong(approximation_concept_id),
convertLong(property_type_concept_id)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "id",
                       "address_line_1",
                       "address_line_2",
                       "address_line_3",
                       "address_line_4",
                       "postcode",
                       "uprn",
                       "approximation_concept_id",
                     "property_type_concept_id"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    Integer.class,
                    String.class,
                    String.class,
                    String.class,
                    String.class,
                    String.class,
                    Long.class,
                    Long.class,
                    Long.class
    }; 
}
}
