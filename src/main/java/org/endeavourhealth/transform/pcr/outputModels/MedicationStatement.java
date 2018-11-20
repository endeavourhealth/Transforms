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
                    Long patientId,
                    Long drugConceptId,
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
                    Long typeConceptId,
                    Long medicationAmountId,
                    Integer issuesAuthorised,
                    Date reviewDate,
                    Integer courseLengthPerIssueDays,
                    Long patientInstructionsFreeTextId,
                    Long pharmacyInstructionsFreeTextId,
                    Boolean isActive,
                    Date endDate,
                    Long endReasonConceptId,
                    Long endReasonFreeTextId,
                    Integer issues,
Boolean isConsent
                    ) throws Exception {
    super.printRecord(OutputContainer.UPSERT,
                    convertLong(id),
                    convertLong(patientId),
                    convertLong(drugConceptId),
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
                    convertLong(typeConceptId),
                    convertLong(medicationAmountId),
                    convertInt(issuesAuthorised),
                    convertDate(reviewDate),
                    convertInt(courseLengthPerIssueDays),
                    convertLong(patientInstructionsFreeTextId),
                    convertLong(pharmacyInstructionsFreeTextId),
                    convertBoolean(isActive),
                    convertDate(endDate),
                    convertLong(endReasonConceptId),
                    convertLong(endReasonFreeTextId),
                    convertInt(issues),
                    convertBoolean(isConsent)
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
                    String.class,
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
