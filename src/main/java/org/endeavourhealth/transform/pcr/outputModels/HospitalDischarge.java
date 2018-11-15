package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
 
public class HospitalDischarge extends AbstractPcrCsvWriter {
 
 
 
  public HospitalDischarge(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Long id,
                    Long patientId,
                    Long careEpisodeId,
                    Date effectiveDate,
                    Integer effectiveDatePrecision,
                    Long effectivePractitionerId,
                    Long enteredByPractitionerId,
                    Date endDate,
                    Long owningOrganisationId,
                    Long statusConceptId,
                    Boolean isConfidential,
                    Long reasonConceptId,
                    Long freeTextId,
                    Long locationId,
Boolean isConsent
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertLong(id),
                    convertLong(patientId),
                    convertLong(careEpisodeId),
                    convertDate(effectiveDate),
                    convertInt(effectiveDatePrecision),
                    convertLong(effectivePractitionerId),
                    convertLong(enteredByPractitionerId),
                    convertDate(endDate),
                    convertLong(owningOrganisationId),
                    convertLong(statusConceptId),
                    convertBoolean(isConfidential),
                    convertLong(reasonConceptId),
                    convertLong(freeTextId),
                    convertLong(locationId),
                    convertBoolean(isConsent)
       );
}
@Override
public String[] getCsvHeaders() {
   return new String[]{ 
            "save_mode",
                       "id",
                       "patient_id",
                       "care_episode_id",
                       "effective_date",
                       "effective_date_precision",
                       "effective_practitioner_id",
                       "entered_by_practitioner_id",
                       "end_date",
                       "owning_organisation_id",
                       "status_concept_id",
                       "is_confidential",
                       "reason_concept_id",
                       "free_text_id",
                       "location_id",
                     "is_consent"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    Long.class,
                    Long.class,
                    Long.class,
                    Date.class,
                    Integer.class,
                    Long.class,
                    Long.class,
                    Date.class,
                    Long.class,
                    Long.class,
                    Boolean.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    Boolean.class
    }; 
}
}
