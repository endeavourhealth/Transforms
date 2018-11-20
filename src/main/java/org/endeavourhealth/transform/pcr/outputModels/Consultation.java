package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
import java.math.BigDecimal;
 
public class Consultation extends AbstractPcrCsvWriter {
 
 
 
  public Consultation(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
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
                    Long usualPractitionerId,
                    Long owningOrganisationId,
                    Long statusConceptId,
                    Boolean isConfidential,
                    Long durationMinutes,
                    Long travelTimeMinutes,
                    Long reasonConceptId,
                    Long purposeConceptId,
                    Long outcomeConceptId,
                    Long freeTextId,
                    Long locationId,
                    Long appointmentSlotId,
                    Long referralRequestId,
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
                    convertLong(usualPractitionerId),
                    convertLong(owningOrganisationId),
                    convertLong(statusConceptId),
                    convertBoolean(isConfidential),
                    convertLong(durationMinutes),
                    convertLong(travelTimeMinutes),
                    convertLong(reasonConceptId),
                    convertLong(purposeConceptId),
                    convertLong(outcomeConceptId),
                    convertLong(freeTextId),
                    convertLong(locationId),
                    convertLong(appointmentSlotId),
                    convertLong(referralRequestId),
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
                       "usual_practitioner_id",
                       "owning_organisation_id",
                       "status_concept_id",
                       "is_confidential",
                       "duration_minutes",
                       "travel_time_minutes",
                       "reason_concept_id",
                       "purpose_concept_id",
                       "outcome_concept_id",
                       "free_text_id",
                       "location_id",
                       "appointment_slot_id",
                       "referral_request_id",
                     "is_consent"
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
                    Long.class,
                    Long.class,
                    Long.class,
                    Boolean.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    Boolean.class
    }; 
}
}
