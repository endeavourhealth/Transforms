package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
 
public class Medication_order extends AbstractPcrCsvWriter {
 
 
 
  public Medication_order(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
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
                    Long medication_statement_id,
                    Long medication_amount_id,
                    Long patient_instructions_free_text_id,
                    Long pharmacy_instructions_free_text_id,
                    Double estimated_cost,
                    Boolean is_active,
                    Integer duration_days,
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
                    convertLong(medication_statement_id),
                    convertLong(medication_amount_id),
                    convertLong(patient_instructions_free_text_id),
                    convertLong(pharmacy_instructions_free_text_id),
                    convertDouble(estimated_cost),
                    convertBoolean(is_active),
                    convertInt(duration_days),
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
                       "medication_statement_id",
                       "medication_amount_id",
                       "patient_instructions_free_text_id",
                       "pharmacy_instructions_free_text_id",
                       "estimated_cost",
                       "is_active",
                       "duration_days",
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
                    Long.class,
                    Long.class,
                    Long.class,
                    Double.class,
                    Boolean.class,
                    Integer.class,
                    Boolean.class
    }; 
}
}
