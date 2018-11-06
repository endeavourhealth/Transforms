package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;

import java.util.Date;

public class Patient extends AbstractPcrCsvWriter {



    public Patient(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);


    }

    public void writeDelete(long id) throws Exception {

        super.printRecord(OutputContainer.DELETE,
                "" + id);
    }


    public void writeUpsert(long id,
                            long organizationId,
                            String nhsNumber,
                            long nhsNumberVerificationConceptId,
                            Date dateOfBirth,
                            Date dateOfDeath,
                            long genderConceptId,
                            long usualPractitionerId,
                            long careProviderId,
                            String title,
                            String firstName,
                            String middleNames,
                            String lastName,
                            String previousLastName,
                            long homeAddressId,
     //TODO in DB or not?                       String ethnicCode,
                            boolean isSpineSensitive) throws Exception {

        super.printRecord(OutputContainer.UPSERT,
                "" + id,
                "" + organizationId,
                nhsNumber,
                convertLong(nhsNumberVerificationConceptId),
                convertDate(dateOfBirth),
                convertDate(dateOfDeath),
                "" + genderConceptId,
                convertLong(usualPractitionerId),
                convertLong(careProviderId),
                title,
                firstName,
                middleNames,
                lastName,
                previousLastName,
                convertLong(homeAddressId),
    //            ethnicCode,
                convertBoolean(isSpineSensitive));
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[]{
                "save_mode",
                "id",
                "organisation_id",
                "nhs_number",
                "nhs_number_verification_concept_id",
                "date_of_birth",
                "date_of_death",
                "gender_concept_id",
                "usual_practitioner_id",
                "care_provider",
                "title",
                "first_name",
                "middle_names",
                "last_name",
                "previous_last_name",
                "home_address_id",
     //           "ethnic_code",
                "is_spine_sensitive"
        };
    }


    @Override
    public Class[] getColumnTypes() {
        return new Class[]{
                String.class,
                Long.TYPE,
                Long.TYPE,
                String.class,
                Long.TYPE,
                Date.class,
                Date.class,
                Long.TYPE,
                Long.TYPE,
                Long.TYPE,
                String.class,
                String.class,
                String.class,
                String.class,
                String.class,
                Long.class,
    //            String.class,
                Long.class,
                boolean.class
        };
    }


}
