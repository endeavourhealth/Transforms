package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
 
public class Gp_registration_status extends AbstractPcrCsvWriter {
 
 
 
  public Gp_registration_status(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Integer id,
                    Integer patient_id,
                    Integer owning_organisation_id,
                    Date effective_date,
                    Integer effective_date_precision,
                    Integer effective_practitioner_id,
                    Date end_date,
                    Long gp_registration_type_concept_id,
                    Long gp_registration_status_concept_id,
                    Long gp_registration_status_sub_concept_id,
Boolean is_current
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertInt(id),
                    convertInt(patient_id),
                    convertInt(owning_organisation_id),
                    convertDate(effective_date),
                    convertInt(effective_date_precision),
                    convertInt(effective_practitioner_id),
                    convertDate(end_date),
                    convertLong(gp_registration_type_concept_id),
                    convertLong(gp_registration_status_concept_id),
                    convertLong(gp_registration_status_sub_concept_id),
convertBoolean(is_current)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "id",
                       "patient_id",
                       "owning_organisation_id",
                       "effective_date",
                       "effective_date_precision",
                       "effective_practitioner_id",
                       "end_date",
                       "gp_registration_type_concept_id",
                       "gp_registration_status_concept_id",
                       "gp_registration_status_sub_concept_id",
                     "is_current"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    Integer.class,
                    Integer.class,
                    Integer.class,
                    Date.class,
                    Integer.class,
                    Integer.class,
                    Date.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    Boolean.class
    }; 
}
}
