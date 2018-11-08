package org.endeavourhealth.transform.pcr.outputModels.UnusedSoFar;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.endeavourhealth.transform.pcr.outputModels.OutputContainer;

import java.util.Date;
 
public class Care_episode_additional_practitioner extends AbstractPcrCsvWriter {
 
 
 
  public Care_episode_additional_practitioner(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Integer patient_id,
                    Long care_episode_id,
Integer practitioner_id
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertInt(patient_id),
                    convertLong(care_episode_id),
convertInt(practitioner_id)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "patient_id",
                       "care_episode_id",
                     "practitioner_id"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    Integer.class,
                    Long.class,
                    Integer.class
    }; 
}
}
