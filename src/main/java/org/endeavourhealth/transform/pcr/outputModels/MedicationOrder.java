package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;

import java.math.BigDecimal;
import java.util.Date;

public class MedicationOrder extends AbstractPcrCsvWriter {

    public MedicationOrder(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
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
                            long careActivityHeadingConceptId,
                            long owningOrganisationId,
                            long statusConceptId,
                            boolean confidential,
                            long typeConceptId,
                            Long medicationStatementId,
                            Long medicationAmountId,
                            long patientInstructionsFreeTextId,
                            long pharmacyInstructionsFreeTextId,
                            BigDecimal estimatedCost,
                            boolean isActive,
                            Integer durationDays,
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
                convertLong(careActivityHeadingConceptId),
                "" + owningOrganisationId,
                convertLong(statusConceptId),
                convertBoolean(confidential),
                convertLong(typeConceptId),
                convertLong(medicationStatementId),
                convertLong(medicationAmountId),
                convertLong(patientInstructionsFreeTextId),
                convertLong(pharmacyInstructionsFreeTextId),
                convertBigDecimal(estimatedCost),
                convertBoolean(isActive),
                convertInt(durationDays),
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
                "medication_statement_id",
                "medication_amount_id",
                "patient_instructions_free_text_id",
                "pharmacy_instructions_free_text_id",
                "estimated_cost",
                "is_active",
                "duration_days",
                "is_consent"
        };
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[] {
                String.class,
                Long.TYPE,
                Integer.class,
                Long.class,
                Date.class,
                Integer.class,
                Integer.class,
                Date.class,
                Date.class,
                Integer.class,
                Long.TYPE,
                Long.class,
                Integer.class,
                Long.class,
                Boolean.TYPE,
                Long.class,
                Long.TYPE,
                Long.TYPE,
                Long.TYPE,
                Long.TYPE,
                BigDecimal.class,
                Boolean.TYPE,
                Integer.class,
                Boolean.TYPE
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
//            medication_statement_id bigint COMMENT 'refers to the medication_statement table',
//            medication_amount_id bigint COMMENT 'refers to the medication_amount table for the dose and quantity',
//            patient_instructions_free_text_id bigint COMMENT 'links to free text entry giving additional patient instructions',
//            pharmacy_instructions_free_text_id bigint COMMENT 'links to free text entry giving additional pharmacist instructions',
//            estimated_cost double,
//            is_active boolean,
//            duration_days int,
//            is_consent boolean NOT NULL COMMENT 'whether consent or dissent'
