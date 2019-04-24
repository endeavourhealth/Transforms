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
                                         int patientGenderId,
                                         String pseudoId,
                                         Integer ageYears,
                                         Integer ageMonths,
                                         Integer ageWeeks,
                                         Date dateOfDeath,
                                         String postcodePrefix,
                                         String lsoaCode,
                                         String msoaCode,
                                         String ethnicCode,
                                         String wardCode,
                                         String localAuthorityCode,
                                         Long registeredPracticeId) throws Exception {

        super.printRecord(OutputContainer.UPSERT,
                "" + id,
                "" + patientGenderId,
                pseudoId,
                convertInt(ageYears),
                convertInt(ageMonths),
                convertInt(ageWeeks),
                convertDate(dateOfDeath),
                postcodePrefix,
                lsoaCode,
                msoaCode,
                ethnicCode,
                wardCode,
                localAuthorityCode,
                convertLong(registeredPracticeId));
    }


    public void writeUpsertIdentifiable(long id,
                                        int patientGenderId,
                                        String nhsNumber,
                                        Date dateOfBirth,
                                        Date dateOfDeath,
                                        String postcode,
                                        String lsoaCode,
                                        String msoaCode,
                                        String ethnicCode,
                                        String wardCode,
                                        String localAuthorityCode,
                                        Long registeredPracticeId,
                                        String title,
                                        String firstNames,
                                        String lastName) throws Exception {

        super.printRecord(OutputContainer.UPSERT,
                "" + id,
                "" + patientGenderId,
                nhsNumber,
                convertDate(dateOfBirth),
                convertDate(dateOfDeath),
                postcode,
                lsoaCode,
                msoaCode,
                ethnicCode,
                wardCode,
                localAuthorityCode,
                convertLong(registeredPracticeId),
                title,
                firstNames,
                lastName);
    }

    @Override
    public String[] getCsvHeaders() {
        if (isPseduonymised()) {
            return new String[] {
                    "save_mode",
                    "id",
                    "patient_gender_id",
                    "pseudo_id",
                    "age_years",
                    "age_months",
                    "age_weeks",
                    "date_of_death",
                    "postcode_prefix",
                    "lsoa_code",
                    "msoa_code",
                    "ethnic_code",
                    "ward_code",
                    "local_authority_code",
                    "registered_practice_organization_id"
            };
        } else {
            return new String[]{
                    "save_mode",
                    "id",
                    "patient_gender_id",
                    "nhs_number",
                    "date_of_birth",
                    "date_of_death",
                    "postcode",
                    "lsoa_code",
                    "msoa_code",
                    "ethnic_code",
                    "ward_code",
                    "local_authority_code",
                    "registered_practice_organization_id",
                    "title",
                    "first_names",
                    "last_name"
            };
        }
    }

    @Override
    public Class[] getColumnTypes() {
        if (isPseduonymised()) {
            return new Class[] {
                    String.class,
                    Long.TYPE,
                    Integer.TYPE,
                    String.class,
                    Integer.class,
                    Integer.class,
                    Integer.class,
                    Date.class,
                    String.class,
                    String.class,
                    String.class,
                    String.class,
                    String.class,
                    String.class,
                    Long.class
            };
        } else {
            return new Class[] {
                    String.class,
                    Long.TYPE,
                    Integer.TYPE,
                    String.class,
                    Date.class,
                    Date.class,
                    String.class,
                    String.class,
                    String.class,
                    String.class,
                    String.class,
                    String.class,
                    Long.class,
                    String.class,
                    String.class,
                    String.class
            };
        }
    }

}
