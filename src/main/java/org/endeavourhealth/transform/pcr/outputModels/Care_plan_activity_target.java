package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
 
public class Care_plan_activity_target extends AbstractPcrCsvWriter {
 
 
 
  public Care_plan_activity_target(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Integer id,
                    Integer care_plan_activity_id,
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
                    Long target_concept_id,
                    Date target_date,
                    Long outcome_concept_id,
                    Date outcome_date,
Boolean is_consent
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertInt(id),
                    convertInt(care_plan_activity_id),
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
                    convertLong(target_concept_id),
                    convertDate(target_date),
                    convertLong(outcome_concept_id),
                    convertDate(outcome_date),
convertBoolean(is_consent)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "id",
                       "care_plan_activity_id",
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
                       "target_concept_id",
                       "target_date",
                       "outcome_concept_id",
                       "outcome_date",
                     "is_consent"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    Integer.class,
                    Integer.class,
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
                    Date.class,
                    Long.class,
                    Date.class,
                    Boolean.class
    }; 
}
}
