package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;

import java.util.Date;
 
public class Immunisation extends AbstractPcrCsvWriter {
 
 
 
  public Immunisation(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
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
                    String dose,
                    Long bodyLocationConceptId,
                    Long methodConceptId,
                    String batchNumber,
                    Date expiryDate,
                    String manufacturer,
                    Integer doseOrdinal,
                    Integer dosesRequired,
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
                    dose,
                    convertLong(bodyLocationConceptId),
                    convertLong(methodConceptId),
                    batchNumber,
                    convertDate(expiryDate),
                    manufacturer,
                    convertInt(doseOrdinal),
                    convertInt(dosesRequired),
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
                       "dose",
                       "body_location_concept_id",
                       "method_concept_id",
                       "batch_number",
                       "expiry_date",
                       "manufacturer",
                       "dose_ordinal",
                       "doses_required",
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
                    String.class,
                    Long.class,
                    Long.class,
                    String.class,
                    Date.class,
                    String.class,
                    Integer.class,
                    Integer.class,
                    Boolean.class
    }; 
}
}
