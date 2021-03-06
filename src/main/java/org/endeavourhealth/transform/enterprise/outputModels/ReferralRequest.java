package org.endeavourhealth.transform.enterprise.outputModels;

import org.apache.commons.csv.CSVFormat;

import java.util.Date;

public class ReferralRequest extends AbstractEnterpriseCsvWriter {

    private boolean includeDateRecorded;

    public ReferralRequest(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
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
                            long organizationId,
                            long patientId,
                            long personId,
                            Long encounterId,
                            Long practitionerId,
                            Date clinicalEffectiveDate,
                            Integer datePrecisionId,
                            Long snomedConceptId,
                            Long requesterOrganizationId,
                            Long recipientOrganizationId,
                            Integer priorityId,
                            Integer typeId,
                            String mode,
                            Boolean outgoing,
                            String originalCode,
                            String originalTerm,
                            boolean isReview,
                            String speciality,
                            String ubrn,
                            Date dateRecorded) throws Exception {

        if (includeDateRecorded) {
            super.printRecord(OutputContainer.UPSERT,
                    "" + id,
                    "" + organizationId,
                    "" + patientId,
                    "" + personId,
                    convertLong(encounterId),
                    convertLong(practitionerId),
                    convertDate(clinicalEffectiveDate),
                    convertInt(datePrecisionId),
                    convertLong(snomedConceptId),
                    convertLong(requesterOrganizationId),
                    convertLong(recipientOrganizationId),
                    convertInt(priorityId),
                    convertInt(typeId),
                    mode,
                    convertBoolean(outgoing),
                    originalCode,
                    originalTerm,
                    convertBoolean(isReview),
                    speciality,
                    ubrn,
                    convertDateTime(dateRecorded));
        } else {
            super.printRecord(OutputContainer.UPSERT,
                    "" + id,
                    "" + organizationId,
                    "" + patientId,
                    "" + personId,
                    convertLong(encounterId),
                    convertLong(practitionerId),
                    convertDate(clinicalEffectiveDate),
                    convertInt(datePrecisionId),
                    convertLong(snomedConceptId),
                    convertLong(requesterOrganizationId),
                    convertLong(recipientOrganizationId),
                    convertInt(priorityId),
                    convertInt(typeId),
                    mode,
                    convertBoolean(outgoing),
                    originalCode,
                    originalTerm,
                    convertBoolean(isReview),
                    speciality,
                    ubrn);
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
                    "requester_organization_id",
                    "recipient_organization_id",
                    "priority_id",
                    "type_id",
                    "mode",
                    "outgoing_referral",
                    "original_code",
                    "original_term",
                    "is_review",
                    "specialty",
                    "ubrn",
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
                    "requester_organization_id",
                    "recipient_organization_id",
                    "priority_id",
                    "type_id",
                    "mode",
                    "outgoing_referral",
                    "original_code",
                    "original_term",
                    "is_review",
                    "specialty",
                    "ubrn",
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
                    Long.class,
                    Long.class,
                    Integer.class,
                    Integer.class,
                    String.class,
                    Boolean.class,
                    String.class,
                    String.class,
                    Boolean.TYPE,
                    String.class,
                    String.class,
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
                    Long.class,
                    Long.class,
                    Integer.class,
                    Integer.class,
                    String.class,
                    Boolean.class,
                    String.class,
                    String.class,
                    Boolean.TYPE,
                    String.class,
                    String.class
            };
        }
    }
}
