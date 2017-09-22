package org.endeavourhealth.transform.enterprise.outputModels;

import org.apache.commons.csv.CSVFormat;

import java.math.BigDecimal;
import java.util.Date;

public class Observation extends AbstractEnterpriseCsvWriter {

    //temporary flag to handle this column being missing from the DB in AIMES
    private boolean hasProblemEndDate = false;

    public Observation(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat, boolean hasProblemEndDate) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);

        this.hasProblemEndDate = hasProblemEndDate;
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
                            BigDecimal value,
                            String units,
                            String originalCode,
                            boolean isProblem,
                            String originalTerm,
                            boolean isReview,
                            Date problemEndDate) throws Exception {

        if (hasProblemEndDate) {
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
                    convertBigDecimal(value),
                    units,
                    originalCode,
                    convertBoolean(isProblem),
                    originalTerm,
                    convertBoolean(isReview),
                    convertDate(problemEndDate));

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
                    convertBigDecimal(value),
                    units,
                    originalCode,
                    convertBoolean(isProblem),
                    originalTerm,
                    convertBoolean(isReview));
        }
    }

    @Override
    public String[] getCsvHeaders() {
        if (hasProblemEndDate) {
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
                    "value",
                    "units",
                    "original_code",
                    "is_problem",
                    "original_term",
                    "is_review",
                    "problem_end_date"
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
                    "value",
                    "units",
                    "original_code",
                    "is_problem",
                    "original_term",
                    "is_review"
            };
        }
    }

    @Override
    public Class[] getColumnTypes() {
        if (hasProblemEndDate) {
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
                    String.class,
                    Boolean.TYPE,
                    String.class,
                    Boolean.TYPE,
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
                    String.class,
                    Boolean.TYPE,
                    String.class,
                    Boolean.TYPE
            };
        }

    }
}
