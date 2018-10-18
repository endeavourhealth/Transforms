package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;

import java.util.Date;

public class Patient extends AbstractPcrCsvWriter {



    public Patient(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat, boolean pseduonymised) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);


    }

    public void writeDelete(long id) throws Exception {

        super.printRecord(OutputContainer.DELETE,
                "" + id);
    }


    public void writeUpsert(long id,
                            long organizationId,
                            String nhsNumber,
                            int nhsNumberVerificationTermId,
                            Date dateOfBirth,
                            Date dateOfDeath,
                            int patientGenderId,
                            long usual_practitioner_id,
                            String title,
                            String firstName,
                            String middleNames,
                            String lastName,
                            String previousLastName,
                            long homeAddressId,
                            String ethnicCode,
                            long careProviderId,
                            boolean isSpineSensitive) throws Exception {

        super.printRecord(OutputContainer.UPSERT,
                "" + id,
                "" + organizationId,
                nhsNumber,
                convertDate(dateOfBirth),
                convertDate(dateOfDeath),
                "" + patientGenderId,
                convertLong(usual_practitioner_id),
                title,
                firstName,
                middleNames,
                lastName,
                previousLastName,
                convertLong(homeAddressId),
                ethnicCode,
                convertLong(careProviderId),
                convertBoolean(isSpineSensitive));
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[]{
                "save_mode",
                "id",
                "organisation_id",
                "nhs_number",
                "nhs_number_verification_term_id",
                "date_of_birth",
                "date_of_death",
                "gender_term_id",
                "usual_practitioner_id",
                "title",
                "first_name",
                "middle_names",
                "last_name",
                "previous_last_name",
                "home_address_id",
                "ethnic_code",
                "care_provider_id",
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
                Integer.TYPE,
                Date.class,
                Date.class,
                Integer.TYPE,
                Long.TYPE,
                String.class,
                String.class,
                String.class,
                String.class,
                String.class,
                Long.class,
                String.class,
                Long.class,
                boolean.class
        };
    }


}
