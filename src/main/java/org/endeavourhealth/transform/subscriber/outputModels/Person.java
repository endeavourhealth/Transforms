package org.endeavourhealth.transform.subscriber.outputModels;

import org.apache.commons.csv.CSVFormat;

import java.util.Date;

public class Person extends AbstractSubscriberCsvWriter {

    private boolean pseduonymised = false;

    public Person(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat, boolean pseduonymised) throws Exception {
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
                                         String title,
                                         String firstNames,
                                         String lastName,
                                         Integer genderConceptId,
                                         String nhsNumber,
                                         Date dateOfBirth,
                                         Date dateOfDeath,
                                         String postcode,
                                         Integer ethnicCodeConceptId,
                                         Long registeredPracticeId) throws Exception {

        super.printRecord(OutputContainer.UPSERT,
                "" + id,
                "" + organizationId,
                null,
                null,
                null,
                convertInt(genderConceptId),
                null,
                convertDate(dateOfBirth),
                convertDate(dateOfDeath),
                postcode,
                convertInt(ethnicCodeConceptId),
                convertLong(registeredPracticeId));
    }


    public void writeUpsertIdentifiable(long id,
                                        long organizationId,
                                        String title,
                                        String firstNames,
                                        String lastName,
                                        Integer genderConceptId,
                                        String nhsNumber,
                                        Date dateOfBirth,
                                        Date dateOfDeath,
                                        String postcode,
                                        Integer ethnicCodeConceptId,
                                        Long registeredPracticeId) throws Exception {

        super.printRecord(OutputContainer.UPSERT,
                "" + id,
                "" + organizationId,
                title,
                firstNames,
                lastName,
                convertInt(genderConceptId),
                nhsNumber,
                convertDate(dateOfBirth),
                convertDate(dateOfDeath),
                postcode,
                convertInt(ethnicCodeConceptId),
                convertLong(registeredPracticeId));
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[]{
                "save_mode",
                "id",
                "organization_id",
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
                String.class,
                String.class,
                String.class,
                Integer.class,
                String.class,
                Date.class,
                Date.class,
                String.class,
                Integer.class,
                Long.TYPE,
        };
    }

}
