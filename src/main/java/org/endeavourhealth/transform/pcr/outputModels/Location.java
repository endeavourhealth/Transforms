package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
 
public class Location extends AbstractPcrCsvWriter {
 
 
 
  public Location(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Long id,
                    Long organisationId,
                    String name,
                    Long typeConceptId,
                    Long addressId,
                    Date startDate,
                    Date endDate,
                    Boolean isActive,
Long parentLocationId
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertLong(id),
                    convertLong(organisationId),
                    name,
                    convertLong(typeConceptId),
                    convertLong(addressId),
                    convertDate(startDate),
                    convertDate(endDate),
                    convertBoolean(isActive),
                    convertLong(parentLocationId)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "id",
                       "organisation_id",
                       "name",
                       "type_concept_id",
                       "address_id",
                       "start_date",
                       "end_date",
                       "is_active",
                     "parent_location_id"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    Long.class,
                    Long.class,
                    String.class,
                    Long.class,
                    Long.class,
                    Date.class,
                    Date.class,
                    Boolean.class,
                    Long.class
    }; 
}
}
