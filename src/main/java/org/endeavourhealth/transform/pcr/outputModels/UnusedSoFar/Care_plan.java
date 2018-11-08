package org.endeavourhealth.transform.pcr.outputModels.UnusedSoFar;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.endeavourhealth.transform.pcr.outputModels.OutputContainer;

import java.util.Date;
 
public class Care_plan extends AbstractPcrCsvWriter {
 
 
 
  public Care_plan(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Long id,
                    Integer patient_id,
                    Long concept_id,
                    Date effective_date,
                    Integer effective_date_precision,
                    Integer effective_practitioner_id,
                    Date end_date,
                    Long care_activity_id,
                    Long care_activity_heading_concept_id,
                    Integer owning_organisation_id,
                    Long status_concept_id,
                    Boolean is_confidential,
                    Long description_free_text_id,
                    Integer performance_frequency_value,
                    Integer performance_frequency_unit,
                    Long performance_location_concept_id,
                    Integer parent_care_plan,
                    Integer follow_up_event_id,
Boolean is_consent
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertLong(id),
                    convertInt(patient_id),
                    convertLong(concept_id),
                    convertDate(effective_date),
                    convertInt(effective_date_precision),
                    convertInt(effective_practitioner_id),
                    convertDate(end_date),
                    convertLong(care_activity_id),
                    convertLong(care_activity_heading_concept_id),
                    convertInt(owning_organisation_id),
                    convertLong(status_concept_id),
                    convertBoolean(is_confidential),
                    convertLong(description_free_text_id),
                    convertInt(performance_frequency_value),
                    convertInt(performance_frequency_unit),
                    convertLong(performance_location_concept_id),
                    convertInt(parent_care_plan),
                    convertInt(follow_up_event_id),
convertBoolean(is_consent)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "id",
                       "patient_id",
                       "concept_id",
                       "effective_date",
                       "effective_date_precision",
                       "effective_practitioner_id",
                       "end_date",
                       "care_activity_id",
                       "care_activity_heading_concept_id",
                       "owning_organisation_id",
                       "status_concept_id",
                       "is_confidential",
                       "description_free_text_id",
                       "performance_frequency_value",
                       "performance_frequency_unit",
                       "performance_location_concept_id",
                       "parent_care_plan",
                       "follow_up_event_id",
                     "is_consent"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    Long.class,
                    Integer.class,
                    Long.class,
                    Date.class,
                    Integer.class,
                    Integer.class,
                    Date.class,
                    Long.class,
                    Long.class,
                    Integer.class,
                    Long.class,
                    Boolean.class,
                    Long.class,
                    Integer.class,
                    Integer.class,
                    Long.class,
                    Integer.class,
                    Integer.class,
                    Boolean.class
    }; 
}
}
