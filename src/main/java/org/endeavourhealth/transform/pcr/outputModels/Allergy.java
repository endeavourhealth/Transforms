package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
 
public class Allergy extends AbstractPcrCsvWriter {
 
 
 
  public Allergy(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
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
                    Integer owning_organisation_id,
                    Long status_concept_id,
                    Boolean is_confidential,
                    Long substance_concept_id,
                    Long manifestation_concept_id,
                    Long manifestation_free_text_id,
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
                    convertInt(owning_organisation_id),
                    convertLong(status_concept_id),
                    convertBoolean(is_confidential),
                    convertLong(substance_concept_id),
                    convertLong(manifestation_concept_id),
                    convertLong(manifestation_free_text_id),
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
                       "status_concept_id",
                       "is_confidential",
                       "substance_concept_id",
                       "manifestation_concept_id",
                       "manifestation_free_text_id",
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
                    Boolean.class
    }; 
}
}
