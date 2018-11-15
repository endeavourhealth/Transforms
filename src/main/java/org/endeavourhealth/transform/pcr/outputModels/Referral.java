package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
 
import java.util.Date;
 
public class Referral extends AbstractPcrCsvWriter {
 
 
 
  public Referral(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
 
 
    }
 
 public void writeDelete(long id) throws Exception {
        super.printRecord(OutputContainer.DELETE,
                "" + id);
}
    public void writeUpsert(Long id,
                    Long patientId,
                    Long conceptId,
                    Date effectiveDate,
                    Integer effectiveDatePrecision,
                    Long effectivePractitionerId,
                    Long enteredByPractitionerId,
                    Long careActivityId,
                    Long careActivityHeadingConceptId,
                    Long owningOrganisationId,
                    Long statusConceptId,
                    Boolean isConfidential,
                    String originalCode,
                    String originalTerm,
                    Integer originalCodeScheme,
                    Integer originalSystem,
                    String ubrn,
                    Long priorityConceptId,
                    Long senderOrganisationId,
                    Long recipientOrganisationId,
                    Long modeConceptId,
                    Long sourceConceptId,
                    Long serviceRequestedConceptId,
                    Long reasonForReferralFreeTextId,
Boolean isConsent
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertLong(id),
                    convertLong(patientId),
                    convertLong(conceptId),
                    convertDate(effectiveDate),
                    convertInt(effectiveDatePrecision),
                    convertLong(effectivePractitionerId),
                    convertLong(enteredByPractitionerId),
                    convertLong(careActivityId),
                    convertLong(careActivityHeadingConceptId),
                    convertLong(owningOrganisationId),
                    convertLong(statusConceptId),
                    convertBoolean(isConfidential),
                    originalCode,
                    originalTerm,
                    convertInt(originalCodeScheme),
                    convertInt(originalSystem),
                    ubrn,
                    convertLong(priorityConceptId),
                    convertLong(senderOrganisationId),
                    convertLong(recipientOrganisationId),
                    convertLong(modeConceptId),
                    convertLong(sourceConceptId),
                    convertLong(serviceRequestedConceptId),
                    convertLong(reasonForReferralFreeTextId),
                    convertBoolean(isConsent)
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
                       "entered_by_practitioner_id",
                       "care_activity_id",
                       "care_activity_heading_concept_id",
                       "owning_organisation_id",
                       "status_concept_id",
                       "is_confidential",
                       "original_code",
                       "original_term",
                       "original_code_scheme",
                       "original_system",
                       "ubrn",
                       "priority_concept_id",
                       "sender_organisation_id",
                       "recipient_organisation_id",
                       "mode_concept_id",
                       "source_concept_id",
                       "service_requested_concept_id",
                       "reason_for_referral_free_text_id",
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
                    Long.class,
                    Long.class,
                    Long.class,
                    Long.class,
                    Boolean.class,
                    String.class,
                    String.class,
                    Integer.class,
                    Integer.class,
                    String.class,
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
