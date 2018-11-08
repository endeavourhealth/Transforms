package org.endeavourhealth.transform.pcr.outputModels.OutputModelsFromEnterprise;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.endeavourhealth.transform.pcr.outputModels.OutputContainer;

import java.util.Date;

public class MedicationStatement extends AbstractPcrCsvWriter {

    public MedicationStatement(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
    }

    public void writeDelete(long id) throws Exception {

        super.printRecord(OutputContainer.DELETE,
                "" + id);
    }

    public void writeUpsert(long id,
                            Integer patientId,
                            long conceptId,
                            Date effectiveDate,
                            Integer effectiveDatePrecisionId,
                            Integer effectivePractitionerId,
                            Date insertDate,
                            Date enteredDate,
                            Integer enteredByPractitionerId,
                            long careActivityId,
                            Integer careActivityHeadingConceptId,
                            long owningOrganisationId,
                            long statusConceptId,
                            boolean confidential,
                            long typeConceptId,
                            long medicationAmountId,
                            Integer issuesAuthorised,
                            Date reviewDate,
                            Integer courseLengthPerIssueDays,
                            long patientInstructionsFreeTextId,
                            long pharmacyInstructionsFreeTextId,
                            boolean isActive,
                            Date endDate,
                            long endReasonConceptId,
                            long endReasonFreeTextId,
                            Integer issues,
                            boolean isConsent) throws Exception {

        super.printRecord(OutputContainer.UPSERT,
                "" + id,
                "" + patientId,
                convertLong(conceptId),
                convertDate(effectiveDate),
                convertInt(effectiveDatePrecisionId),
                convertInt(effectivePractitionerId),
                convertDate(insertDate),
                convertDate(enteredDate),
                convertInt(enteredByPractitionerId),
                convertLong(careActivityId),
                convertInt(careActivityHeadingConceptId),
                "" + owningOrganisationId,
                convertLong(statusConceptId),
                convertBoolean(confidential),
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
                convertBoolean(isConsent));
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[] {
                "save_mode",
                "id",
                "patient_id",
                "drug_concept_id",
                "effective_date",
                "effective_date_precision",
                "effective_practitioner_id",
                "insert_date",
                "entered_date",
                "entered_practitioner_id",
                "care_activity_id",
                "care_activity_heading_concept_id",
                "owning_organisation_id",
                "status_concept_id",
                "is_confidential",
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
        return new Class[] {
                String.class,
                Long.TYPE,
                Long.class,
                Integer.class,
                Date.class,
                Integer.class,
                Integer.class,
                Date.class,
                Date.class,
                Integer.class,
                Long.TYPE,
                Integer.class,
                Integer.class,
                Long.class,
                Boolean.TYPE,
                Integer.class,
                Long.class,
                Integer.class,
                Date.class,
                Integer.class,
                Long.TYPE,
                Long.TYPE,
                Boolean.class,
                Date.class,
                Long.class,
                Long.class,
                Integer.TYPE,
                Boolean.class
        };
    }
}

//            id bigint NOT NULL,
//	          patient_id int NOT NULL,
//            drug_concept_id bigint NOT NULL COMMENT 'refers to information model, giving the clinical concept of the event',
//            effective_date datetime NOT NULL COMMENT 'clinically significant date and time',
//            effective_date_precision tinyint NOT NULL COMMENT 'qualifies the effective_date for display purposes',
//            effective_practitioner_id int COMMENT 'refers to the practitioner table for who is said to have done the event',
//            insert_date datetime NOT NULL COMMENT 'datetime actually inserted, so even if other dates are null, we can order by something',
//            entered_date datetime NOT NULL,
//            entered_practitioner_id int COMMENT 'refers to the practitioner table for who actually entered the data into the host system',
//            care_activity_id bigint COMMENT 'by having this here, we don''t need an care_activity_id on the observation, referral, allergy table etc.',
//            care_activity_heading_concept_id bigint NOT NULL COMMENT 'information model concept describing the care activity heading type (e.g. examination, history)',
//            owning_organisation_id int COMMENT 'refers to the organisation that owns/manages the event',
//            status_concept_id bigint NOT NULL COMMENT 'refers to information model, giving the event status (e.g. active, final, pending, amended, corrected, deleted)',
//            is_confidential boolean NOT NULL COMMENT 'indicates this is a confidential event',
//            type_concept_id bigint NOT NULL COMMENT 'refers to information model to give the prescription type (e.g. Acute, Repeat, RepeatDispensing)',
//            medication_amount_id bigint COMMENT 'refers to the medication_amount table for the dose and quantity',
//            issues_authorised int COMMENT 'total number of issues allowed before review, for acutes this value will be 1',
//            review_date date COMMENT 'date medication needs to be reviewed',
//            course_length_per_issue_days int COMMENT 'number of days each issue of this medication is expected to last',
//            patient_instructions_free_text_id bigint COMMENT 'links to free text entry giving additional patient instructions',
//            pharmacy_instructions_free_text_id bigint COMMENT 'links to free text entry giving additional pharmacist instructions',
//            is_active boolean,
//            end_date date COMMENT 'date medication was stopped',
//            end_reason_concept_id bigint COMMENT 'reason for ending this medication',
//            end_reason_free_text_id bigint COMMENT 'links to free text entry giving detail on why this was ended',
//            issues int COMMENT 'number of issues received',
//            is_consent boolean NOT NULL COMMENT 'whether consent or dissent'
