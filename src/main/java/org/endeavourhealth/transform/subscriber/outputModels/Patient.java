package org.endeavourhealth.transform.subscriber.outputModels;

import org.apache.commons.csv.CSVFormat;

import java.util.Date;

public class Patient extends AbstractSubscriberCsvWriter {

    private boolean pseduonymised = false;

    public Patient(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat, boolean pseduonymised) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);

        this.pseduonymised = pseduonymised;
    }

    public void writeDelete(long id) throws Exception {

        super.printRecord(OutputContainer.DELETE,
                "" + id);
    }

    public boolean isPseduonymised() {
        return pseduonymised;
    }


    public void writeUpsertPseudonymised(long id,
                                         long organizationId,
                                         long personId,
                                         String title,
                                         String firstNames,
                                         String lastName,
                                         int genderConceptId,
                                         String nhsNumber,
                                         Date dateOfBirth,
                                         Date dateOfDeath,
                                         String postcode,
                                         int ethnicCodeConceptId,
                                         Long registeredPracticeId) throws Exception {

        super.printRecord(OutputContainer.UPSERT,
                "" + id,
                "" + organizationId,
                "" + personId,
                null,
                null,
                null,
                "" + genderConceptId,
                null,
                convertDate(dateOfBirth),
                convertDate(dateOfDeath),
                postcode,
                "" + ethnicCodeConceptId,
                convertLong(registeredPracticeId));
    }


    public void writeUpsertIdentifiable(long id,
                                        long organizationId,
                                        long personId,
                                        String title,
                                        String firstNames,
                                        String lastName,
                                        int genderConceptId,
                                        String nhsNumber,
                                        Date dateOfBirth,
                                        Date dateOfDeath,
                                        String postcode,
                                        int ethnicCodeConceptId,
                                        Long registeredPracticeId) throws Exception {

        super.printRecord(OutputContainer.UPSERT,
                "" + id,
                "" + organizationId,
                "" + personId,
                title,
                firstNames,
                lastName,
                "" + genderConceptId,
                nhsNumber,
                convertDate(dateOfBirth),
                convertDate(dateOfDeath),
                postcode,
                "" + ethnicCodeConceptId,
                convertLong(registeredPracticeId));
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[]{
                "save_mode",
                "id",
                "organization_id",
                "person_id",
                "title",
                "first_names",
                "last_name",
                "gender_concept_id",
                "nhs_number",
                "date_of_birth",
                "date_of_death",
                "postcode",
                "ethnic_code_concept_id",
                "registered_practice_organization_id"
        };
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[] {
                String.class,
                Long.TYPE,
                Long.TYPE,
                Long.TYPE,
                String.class,
                String.class,
                String.class,
                Integer.TYPE,
                String.class,
                Date.class,
                Date.class,
                String.class,
                Integer.TYPE,
                Long.TYPE,
        };
    }
}
