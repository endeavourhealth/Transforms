package org.endeavourhealth.transform.pcr.outputModels.OutputModelsFromEnterprise;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.endeavourhealth.transform.pcr.outputModels.OutputContainer;

import java.util.Date;

public class AllergyIntolerance extends AbstractPcrCsvWriter {

    public AllergyIntolerance(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
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
                            String codeSystem,
                            boolean confidential,
                            long substanceConceptId,
                            long manifestationConceptId,
                            long manifestationFreeTextId,
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
                codeSystem,
                convertBoolean(confidential),
                convertLong(substanceConceptId),
                convertLong(manifestationConceptId),
                convertLong(manifestationFreeTextId),
                convertBoolean(isConsent));
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
                "code_system",
                "is_confidential",
                "substance_concept_id",
                "manifestation_concept_id",
                "manifestation_free_text_id",
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
                String.class,
                Boolean.TYPE,
                Long.class,
                Long.class,
                Long.class,
                Boolean.TYPE
        };
    }
}

//              id bigint NOT NULL,
//              patient_id int NOT NULL,
//              concept_id bigint NOT NULL COMMENT 'refers to information model, giving the clinical concept of the event',
//              effective_date datetime NOT NULL COMMENT 'clinically significant date and time',
//              effective_date_precision tinyint NOT NULL COMMENT 'qualifies the effective_date for display purposes',
//              effective_practitioner_id int COMMENT 'refers to the practitioner table for who is said to have done the event',
//              insert_date datetime NOT NULL COMMENT 'datetime actually inserted, so even if other dates are null, we can order by something',
//              entered_date datetime NOT NULL,
//              entered_practitioner_id int COMMENT 'refers to the practitioner table for who actually entered the data into the host system',
//              care_activity_id bigint COMMENT 'by having this here, we don''t need an care_activity_id on the observation, referral, allergy table etc.',
//              care_activity_heading_concept_id bigint NOT NULL COMMENT 'information model concept describing the care activity heading type (e.g. examination, history)',
//              owning_organisation_id int COMMENT 'refers to the organisation that owns/manages the event',
//              status_concept_id bigint NOT NULL COMMENT 'refers to information model, giving the event status (e.g. active, final, pending, amended, corrected, deleted)',
//              is_confidential boolean NOT NULL COMMENT 'indicates this is a confidential event',
//              substance_concept_id bigint COMMENT 'concept representing the substance (as opposed to the allergy code, which is on the observation table)',
//              manifestation_concept_id bigint COMMENT 'concept stating how this allergy manifests itself (e.g. rash, anaphylactic shock)',
//              manifestation_free_text_id bigint COMMENT 'links to free text record to give textual description of the manifestation',
//	            is_consent boolean NOT NULL COMMENT 'whether consent or dissent'