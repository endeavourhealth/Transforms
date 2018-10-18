package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;

import java.util.Date;

public class Observation extends AbstractPcrCsvWriter {

    public Observation(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
    }

    public void writeDelete(long id) throws Exception {

        super.printRecord(OutputContainer.DELETE,
                "" + id);
    }

    public void writeUpsert(long id,
                            Integer patientId,
                            Integer conceptId,
                            Date effectiveDate,
                            Integer effectiveDatePrecisionId,
                            Integer effectivePractitionerId,
                            Date insertDate,
                            Date enteredDate,
                            Integer enteredByPractitionerId,
                            long careActivityId,
                            Integer careActivityHeadingConceptId,
                            long owningOrganisationId,
                            Integer statusConceptId,
                            boolean confidential,
                            String originalCode,
                            String originalTerm,
                            Integer episodicityConceptId,
                            long freeTextId,
                            Integer dataEntryPromptId,
                            Integer significanceConceptId,
                            boolean isConsent) throws Exception {

        super.printRecord(OutputContainer.UPSERT,
                "" + id,
                "" + patientId,
                convertInt(conceptId),
                convertDate(effectiveDate),
                convertInt(effectiveDatePrecisionId),
                convertInt(effectivePractitionerId),
                convertDate(insertDate),
                convertDate(enteredDate),
                convertInt(enteredByPractitionerId),
                convertLong(careActivityId),         //care activity Id, EncounterId?
                convertInt(careActivityHeadingConceptId),  //needs IM mapping from what in FHR?
                "" + owningOrganisationId,
                convertInt(statusConceptId),          //needs IM mapping from what in FHIR
                convertBoolean(confidential),              //map from what?
                originalCode,
                originalTerm,
                convertInt(episodicityConceptId),   //needs IM mapping and tx from FHIR
                convertLong(freeTextId),            //from where in FHIR?
                convertInt(dataEntryPromptId),      //from where in FHIR?
                convertInt(significanceConceptId),  //needs IM mapping and tx from FHIR
                convertBoolean(isConsent));           //from where in FHIR?
    }


    @Override
    public String[] getCsvHeaders() {
        return new String[] {
                "save_mode",
                "id",
                "patient_id",
                "concept_id",
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
                "original_code",
                "original_concept",
                "episodicity_concept_id",
                "free_text_id",
                "data_entry_prompt_id",
                "significance_concept_id",
                "is_consent"
        };
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[] {
                String.class,
                Long.TYPE,
                Integer.class,
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
                Integer.class,
                Boolean.TYPE,
                String.class,
                String.class,
                Integer.class,
                Long.TYPE,
                Integer.class,
                Integer.class,
                Boolean.TYPE
        };

    }
}

//            id bigint NOT NULL,
//            patient_id int NOT NULL,
//            concept_id int NOT NULL COMMENT 'refers to information model, giving the clinical concept of the event',
//            effective_date datetime NOT NULL COMMENT 'clinically significant date and time',
//            effective_date_precision tinyint NOT NULL COMMENT 'qualifies the effective_date for display purposes',
//            effective_practitioner_id int COMMENT 'refers to the practitioner table for who is said to have done the event',
//            insert_date datetime NOT NULL COMMENT 'datetime actually inserted, so even if other dates are null, we can order by something',
//            entered_date datetime NOT NULL,
//            entered_practitioner_id int COMMENT 'refers to the practitioner table for who actually entered the data into the host system',
//            care_activity_id bigint,
//            care_activity_heading_concept_id int NOT NULL COMMENT 'information model concept describing the care activity heading type (e.g. examination, history)',
//            owning_organisation_id int COMMENT 'refers to the organisation that owns/manages the event',
//            status_concept_id int NOT NULL COMMENT 'refers to information model, giving the event status (e.g. active, final, pending, amended, corrected, deleted)',
//            is_confidential boolean NOT NULL COMMENT 'indicates this is a confidential event',
//            original_code varchar(20) DEFAULT NULL,
//            original_concept varchar(1000) DEFAULT NULL,
//            episodicity_concept_id int COMMENT 'refers to information model, giving episode/review (e.g. new episode, review)',
//            free_text_id bigint COMMENT 'refers to free text table where comments are stored',
//            data_entry_prompt_id int COMMENT 'links to the table giving the free-text prompt used to enter this observation',
//            significance_concept_id int COMMENT 'refers to information model to define the significance, severity, normality or priority (e.g. minor, significant, abnormal, urgent, severe, normal)',
//            is_consent boolean NOT NULL COMMENT 'whether consent or dissent',