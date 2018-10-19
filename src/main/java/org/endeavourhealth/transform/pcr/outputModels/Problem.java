package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;

import java.util.Date;

public class Problem extends AbstractPcrCsvWriter {

    public Problem(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
    }

    public void writeDelete(long id) throws Exception {

        super.printRecord(OutputContainer.DELETE,
                "" + id);
    }

    public void writeUpsert(long id,
                            long patientId,
                            long observationId,
                            Integer typeConceptId,
                            Integer significanceConceptId,
                            Integer expectedDurationDays,
                            Date lastReviewDate,
                            Integer lastReviewPractitionerId) throws Exception {

        super.printRecord(OutputContainer.UPSERT,
                "" + id,
                "" + patientId,
                "" + observationId,
                convertInt(typeConceptId),
                convertInt(significanceConceptId),
                convertInt(expectedDurationDays),
                convertDate(lastReviewDate),
                convertInt(lastReviewPractitionerId));
    }


    @Override
    public String[] getCsvHeaders() {
        return new String[] {
                "save_mode",
                "id",
                "patient_id",
                "observation_id",
                "type_concept_id",
                "significance_concept_id",
                "expected_duration_days",
                "last_review_date",
                "last_review_practitioner_id"
        };
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[] {
                String.class,
                Long.TYPE,
                Integer.class,
                Long.TYPE,
                Integer.class,
                Integer.class,
                Integer.class,
                Date.class,
                Integer.class
        };

    }
}


//            id bigint NOT NULL,
//            patient_id int NOT NULL,
//            observation_id bigint NOT NULL,
//            type_concept_id int COMMENT 'refers to information model for problem type (e.g. problem, issue, health admin)',
//            significance_concept_id int COMMENT 'refers to information model to define the significance (e.g. minor, significant)',
//            expected_duration_days int,
//            last_review_date date,
//            last_review_practitioner_id int,
