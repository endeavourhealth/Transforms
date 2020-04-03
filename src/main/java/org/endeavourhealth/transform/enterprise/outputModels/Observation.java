package org.endeavourhealth.transform.enterprise.outputModels;

import org.apache.commons.csv.CSVFormat;

import java.math.BigDecimal;
import java.util.Date;

public class Observation extends AbstractEnterpriseCsvWriter {

    private boolean includeDateRecorded = false;

    public Observation(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
    }

    public void setIncludeDateRecorded(boolean includeDateRecorded) {
        this.includeDateRecorded = includeDateRecorded;
    }

    public void writeDelete(long id) throws Exception {

        super.printRecord(OutputContainer.DELETE,
                "" + id);
    }

    public void writeUpsert(long id,
                            long organisationId,
                            long patientId,
                            long personId,
                            Long encounterId,
                            Long practitionerId,
                            Date clinicalEffectiveDate,
                            Integer datePrecisionId,
                            Long snomedConceptId,
                            BigDecimal resultValue,
                            String resultValueUnits,
                            Date resultDate,
                            String resultText,
                            Long resultConceptId,
                            String originalCode,
                            boolean isProblem,
                            String originalTerm,
                            boolean isReview,
                            Date problemEndDate,
                            Long parentObservationId,
                            Date dateRecorded) throws Exception {

        if (includeDateRecorded) {
            super.printRecord(OutputContainer.UPSERT,
                    "" + id,
                    "" + organisationId,
                    "" + patientId,
                    "" + personId,
                    convertLong(encounterId),
                    convertLong(practitionerId),
                    convertDate(clinicalEffectiveDate),
                    convertInt(datePrecisionId),
                    convertLong(snomedConceptId),
                    convertBigDecimal(resultValue),
                    resultValueUnits,
                    convertDate(resultDate),
                    resultText,
                    convertLong(resultConceptId),
                    originalCode,
                    convertBoolean(isProblem),
                    originalTerm,
                    convertBoolean(isReview),
                    convertDate(problemEndDate),
                    convertLong(parentObservationId),
                    convertDateTime(dateRecorded));
        } else {
            super.printRecord(OutputContainer.UPSERT,
                    "" + id,
                    "" + organisationId,
                    "" + patientId,
                    "" + personId,
                    convertLong(encounterId),
                    convertLong(practitionerId),
                    convertDate(clinicalEffectiveDate),
                    convertInt(datePrecisionId),
                    convertLong(snomedConceptId),
                    convertBigDecimal(resultValue),
                    resultValueUnits,
                    convertDate(resultDate),
                    resultText,
                    convertLong(resultConceptId),
                    originalCode,
                    convertBoolean(isProblem),
                    originalTerm,
                    convertBoolean(isReview),
                    convertDate(problemEndDate),
                    convertLong(parentObservationId));
        }
    }


    @Override
    public String[] getCsvHeaders() {
        if (includeDateRecorded) {
            return new String[] {
                    "save_mode",
                    "id",
                    "organization_id",
                    "patient_id",
                    "person_id",
                    "encounter_id",
                    "practitioner_id",
                    "clinical_effective_date",
                    "date_precision_id",
                    "snomed_concept_id",
                    "result_value",
                    "result_value_units",
                    "result_date",
                    "result_text",
                    "result_concept_id",
                    "original_code",
                    "is_problem",
                    "original_term",
                    "is_review",
                    "problem_end_date",
                    "parent_observation_id",
                    "date_recorded"
            };
        } else {
            return new String[] {
                    "save_mode",
                    "id",
                    "organization_id",
                    "patient_id",
                    "person_id",
                    "encounter_id",
                    "practitioner_id",
                    "clinical_effective_date",
                    "date_precision_id",
                    "snomed_concept_id",
                    "result_value",
                    "result_value_units",
                    "result_date",
                    "result_text",
                    "result_concept_id",
                    "original_code",
                    "is_problem",
                    "original_term",
                    "is_review",
                    "problem_end_date",
                    "parent_observation_id"
            };
        }
    }

    @Override
    public Class[] getColumnTypes() {
        if (includeDateRecorded) {
            return new Class[] {
                    String.class,
                    Long.TYPE,
                    Long.TYPE,
                    Long.TYPE,
                    Long.TYPE,
                    Long.class,
                    Long.class,
                    Date.class,
                    Integer.class,
                    Long.class,
                    BigDecimal.class,
                    String.class,
                    Date.class,
                    String.class,
                    Long.class,
                    String.class,
                    Boolean.TYPE,
                    String.class,
                    Boolean.TYPE,
                    Date.class,
                    Long.class,
                    Date.class
            };
        } else {
            return new Class[] {
                    String.class,
                    Long.TYPE,
                    Long.TYPE,
                    Long.TYPE,
                    Long.TYPE,
                    Long.class,
                    Long.class,
                    Date.class,
                    Integer.class,
                    Long.class,
                    BigDecimal.class,
                    String.class,
                    Date.class,
                    String.class,
                    Long.class,
                    String.class,
                    Boolean.TYPE,
                    String.class,
                    Boolean.TYPE,
                    Date.class,
                    Long.class
            };
        }

    }
}
