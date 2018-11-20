package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
import java.math.BigDecimal;
 
public class CareEpisode extends AbstractPcrCsvWriter {
 
 
 
  public CareEpisode(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Long id,
                    Long patientId,
                    Long owningOrganisationId,
                    Date effectiveDate,
                    Integer effectiveDatePrecision,
                    Long effectivePractitionerId,
                    Long enteredByPractitionerId,
                    Date endDate,
                    String encounterLinkId,
                    Long statusConceptId,
                    Long specialityConceptId,
                    Long adminConceptId,
                    Long reasonConceptId,
                    Long typeConceptId,
                    Long locationId,
                    Long referralRequestId,
                    Boolean isConsent,
Long latestCareEpisodeStatusId
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertLong(id),
                    convertLong(patientId),
                    convertLong(owningOrganisationId),
                    convertDate(effectiveDate),
                    convertInt(effectiveDatePrecision),
                    convertLong(effectivePractitionerId),
                    convertLong(enteredByPractitionerId),
                    convertDate(endDate),
                    encounterLinkId,
                    convertLong(statusConceptId),
                    convertLong(specialityConceptId),
                    convertLong(adminConceptId),
                    convertLong(reasonConceptId),
                    convertLong(typeConceptId),
                    convertLong(locationId),
                    convertLong(referralRequestId),
                    convertBoolean(isConsent),
                    convertLong(latestCareEpisodeStatusId)
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
                       "entered_by_practitioner_id",
                       "end_date",
                       "encounter_link_id",
                       "status_concept_id",
                       "speciality_concept_id",
                       "admin_concept_id",
                       "reason_concept_id",
                       "type_concept_id",
                       "location_id",
                       "referral_request_id",
                       "is_consent",
                     "latest_care_episode_status_id"
    }; 
} 
@Override 
public Class[] getColumnTypes() { 
    return new Class[]{ 
                    String.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    Date.class,
                    Integer.class,
                    Long.class,
                    Long.class,
                    Date.class,
                    String.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    Boolean.class,
                    Long.class
    }; 
}
}
