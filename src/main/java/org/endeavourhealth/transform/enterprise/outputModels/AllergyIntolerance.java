package org.endeavourhealth.transform.enterprise.outputModels;

import org.apache.commons.csv.CSVFormat;

import java.util.Date;

public class AllergyIntolerance extends AbstractEnterpriseCsvWriter {

    private boolean includeDateRecorded = false;

    public AllergyIntolerance(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
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
                            String originalCode,
                            String originalTerm,
                            boolean isReview,
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
                    originalCode,
                    originalTerm,
                    convertBoolean(isReview),
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
                    originalCode,
                    originalTerm,
                    convertBoolean(isReview));
        }
    }

    @Override
    public String[] getCsvHeaders() {
        if (includeDateRecorded) {
            return new String[]{
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
                    "original_code",
                    "original_term",
                    "is_review",
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
                    "original_code",
                    "original_term",
                    "is_review"
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
                    String.class,
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
                    String.class,
                    String.class,
                    Boolean.TYPE
            };

        }
    }
}
