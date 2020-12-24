package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;

import java.util.Date;

public class Patient extends AbstractTargetTable {

    public Patient(CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(csvFormat, dateFormat, timeFormat);

    }

    public void writeDelete(SubscriberId subscriberId) throws Exception {

        super.printRecord(convertBoolean(true),
                "" + subscriberId.getSubscriberId());
    }

    public void writeUpsert(SubscriberId subscriberId,
                            long organizationId,
                            long personId,
                            String title,
                            String firstNames,
                            String lastName,
                            Integer genderConceptId,
                            String nhsNumber,
                            Date dateOfBirth,
                            Date dateOfDeath,
                            Long currentAddressId,
                            Integer ethnicCodeConceptId,
                            Long registeredPracticeId,
                            Integer birthYear,
                            Integer birthMonth,
                            Integer birthWeek) throws Exception {

        super.printRecord(convertBoolean(false),
                "" + subscriberId.getSubscriberId(),
                "" + organizationId,
                "" + personId,
                title,
                firstNames,
                lastName,
                convertInt(genderConceptId),
                nhsNumber,
                convertDate(dateOfBirth),
                convertDate(dateOfDeath),
                convertLong(currentAddressId),
                convertInt(ethnicCodeConceptId),
                convertLong(registeredPracticeId),
                convertInt(birthYear),
                convertInt(birthMonth),
                convertInt(birthWeek));
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[]{
                "is_delete",
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
                "current_address_id",
                "ethnic_code_concept_id",
                "registered_practice_organization_id",
                "birth_year",
                "birth_month",
                "birth_week"
        };
    }

    @Override
    public SubscriberTableId getTableId() {
        return SubscriberTableId.PATIENT;
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[] {
                Byte.TYPE,
                Long.TYPE,
                Long.TYPE,
                Long.TYPE,
                String.class,
                String.class,
                String.class,
                Integer.class,
                String.class,
                Date.class,
                Date.class,
                Long.class,
                Integer.class,
                Long.class,
                Integer.class,
                Integer.class,
                Integer.class
        };
    }
}
