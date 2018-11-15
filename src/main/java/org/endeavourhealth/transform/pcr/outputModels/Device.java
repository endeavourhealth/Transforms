package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
 
public class Device extends AbstractPcrCsvWriter {
 
 
 
  public Device(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Long id,
                    Long organisationId,
                    Long typeConceptId,
                    String serialNumber,
                    String deviceName,
                    String manufacturer,
                    String humanReadableIdentifier,
                    byte[] machineReadableIdentifier,
                    String version,
Long enteredByPractitionerId
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertLong(id),
                    convertLong(organisationId),
                    convertLong(typeConceptId),
                    serialNumber,
                    deviceName,
                    manufacturer,
                    humanReadableIdentifier,
                    convertBytes(machineReadableIdentifier),
                    version,
                    convertLong(enteredByPractitionerId)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "id",
                       "organisation_id",
                       "type_concept_id",
                       "serial_number",
                       "device_name",
                       "manufacturer",
                       "human_readable_identifier",
                       "machine_readable_identifier",
                       "version",
                     "entered_by_practitioner_id"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    Long.class,
                    Long.class,
                    Long.class,
                    String.class,
                    String.class,
                    String.class,
                    String.class,
                    byte[].class,
                    String.class,
                    Long.class
    }; 
}
}
