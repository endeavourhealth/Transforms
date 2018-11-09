package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
 
public class MedicationStatement extends AbstractPcrCsvWriter {
 
 
 
  public MedicationStatement(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Long id,
                    Integer patient_id,
                    Long drug_concept_id,
                    Date effective_date,
                    Integer effective_date_precision,
                    Integer effective_practitioner_id,
                    Long care_activity_id,
                    Long care_activity_heading_concept_id,
                    Integer owning_organisation_id,
                    Long status_concept_id,
                    Boolean is_confidential,
                    Long type_concept_id,
                    Long medication_amount_id,
                    Integer issues_authorised,
                    Date review_date,
                    Integer course_length_per_issue_days,
                    Long patient_instructions_free_text_id,
                    Long pharmacy_instructions_free_text_id,
                    Boolean is_active,
                    Date end_date,
                    Long end_reason_concept_id,
                    Long end_reason_free_text_id,
                    Integer issues,
Boolean is_consent
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertLong(id),
                    convertInt(patient_id),
                    convertLong(drug_concept_id),
                    convertDate(effective_date),
                    convertInt(effective_date_precision),
                    convertInt(effective_practitioner_id),
                    convertLong(care_activity_id),
                    convertLong(care_activity_heading_concept_id),
                    convertInt(owning_organisation_id),
                    convertLong(status_concept_id),
                    convertBoolean(is_confidential),
                    convertLong(type_concept_id),
                    convertLong(medication_amount_id),
                    convertInt(issues_authorised),
                    convertDate(review_date),
                    convertInt(course_length_per_issue_days),
                    convertLong(patient_instructions_free_text_id),
                    convertLong(pharmacy_instructions_free_text_id),
                    convertBoolean(is_active),
                    convertDate(end_date),
                    convertLong(end_reason_concept_id),
                    convertLong(end_reason_free_text_id),
                    convertInt(issues),
convertBoolean(is_consent)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "id",
                       "patient_id",
                       "drug_concept_id",
                       "effective_date",
                       "effective_date_precision",
                       "effective_practitioner_id",
                       "care_activity_id",
                       "care_activity_heading_concept_id",
                       "owning_organisation_id",
                       "status_concept_id",
                       "is_confidential",
                       "type_concept_id",
                       "medication_amount_id",
                       "issues_authorised",
                       "review_date",
                       "course_length_per_issue_days",
                       "patient_instructions_free_text_id",
                       "pharmacy_instructions_free_text_id",
                       "is_active",
                       "end_date",
                       "end_reason_concept_id",
                       "end_reason_free_text_id",
                       "issues",
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
                    Long.class,
                    Long.class,
                    Integer.class,
                    Long.class,
                    Boolean.class,
                    Long.class,
                    Long.class,
                    Integer.class,
                    Date.class,
                    Integer.class,
                    Long.class,
                    Long.class,
                    Boolean.class,
                    Date.class,
                    Long.class,
                    Long.class,
                    Integer.class,
                    Boolean.class
    }; 
}
}
