package org.endeavourhealth.transform.pcr.outputModels.OutputModelsFromEnterprise;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.endeavourhealth.transform.pcr.outputModels.OutputContainer;

import java.util.Date;

public class Observation extends AbstractPcrCsvWriter {
 
 
 
  public Observation(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
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
                    Long care_activity_id,
                    Long care_activity_heading_concept_id,
                    Long owning_organisation_id,
                    Boolean is_confidential,
                    String original_code,
                    String original_concept,
                    Long episodicity_concept_id,
                    Long free_text_id,
                    Integer data_entry_prompt_id,
                    Long significance_concept_id,
Boolean is_consent
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertLong(id),
                    convertInt(patient_id),
                    convertLong(concept_id),
                    convertDate(effective_date),
                    convertInt(effective_date_precision),
                    convertInt(effective_practitioner_id),
                    convertLong(care_activity_id),
                    convertLong(care_activity_heading_concept_id),
                    convertLong(owning_organisation_id),
                    convertBoolean(is_confidential),
                    original_code,
                    original_concept,
                    convertLong(episodicity_concept_id),
                    convertLong(free_text_id),
                    convertInt(data_entry_prompt_id),
                    convertLong(significance_concept_id),
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
                       "care_activity_id",
                       "care_activity_heading_concept_id",
                       "owning_organisation_id",
                       "is_confidential",
                       "original_code",
                       "original_concept",
                       "episodicity_concept_id",
                       "free_text_id",
                       "data_entry_prompt_id",
                       "significance_concept_id",
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
                    Long.class,
                    Boolean.class,
                    String.class,
                    String.class,
                    Long.class,
                    Long.class,
                    Integer.class,
                    Long.class,
                    Boolean.class
    }; 
}
}
