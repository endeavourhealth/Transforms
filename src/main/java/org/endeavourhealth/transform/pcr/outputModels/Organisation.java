package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
import java.math.BigDecimal;
 
public class Organisation extends AbstractPcrCsvWriter {
 
 
 
  public Organisation(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Long id,
                    String serviceId,
                    String systemId,
                    String odsCode,
                    String name,
                    Boolean isActive,
                    Long parentOrganisationId,
                    Long typeConceptId,
Long mainLocationId
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertLong(id),
                    serviceId,
                    systemId,
                    odsCode,
                    name,
                    convertBoolean(isActive),
                    convertLong(parentOrganisationId),
                    convertLong(typeConceptId),
                    convertLong(mainLocationId)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "id",
                       "service_id",
                       "system_id",
                       "ods_code",
                       "name",
                       "is_active",
                       "parent_organisation_id",
                       "type_concept_id",
                     "main_location_id"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    String.class,
                    Long.class,
                    String.class,
                    String.class,
                    String.class,
                    String.class,
                    Boolean.class,
                    Long.class,
                    Long.class,
                    Long.class
    }; 
}
}
