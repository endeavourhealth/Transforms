package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
import java.math.BigDecimal;
 
public class CareEpisodeStatus extends AbstractPcrCsvWriter {
 
 
 
  public CareEpisodeStatus(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Long patientId,
                    Long owningOrganisationId,
                    Long enteredByPractitionerId,
                    Long careEpisodeId,
                    Date startTime,
                    Date endTime,
Long careEpisodeStatusConceptId
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertLong(patientId),
                    convertLong(owningOrganisationId),
                    convertLong(enteredByPractitionerId),
                    convertLong(careEpisodeId),
                    convertDate(startTime),
                    convertDate(endTime),
                    convertLong(careEpisodeStatusConceptId)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "patient_id",
                       "owning_organisation_id",
                       "entered_by_practitioner_id",
                       "care_episode_id",
                       "start_time",
                       "end_time",
                     "care_episode_status_concept_id"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    String.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    Date.class,
                    Date.class,
                    Long.class
    }; 
}
}
