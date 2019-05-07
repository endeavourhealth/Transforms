package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;

import java.util.Date;

public class Person extends AbstractTargetTable {

    public Person(CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(csvFormat, dateFormat, timeFormat);
    }

    public void writeDelete(SubscriberId subscriberId) throws Exception {

        super.printRecord(convertBoolean(true),
                "" + subscriberId.getSubscriberId());
    }


    public void writeUpsert(SubscriberId subscriberId,
                            long organizationId,
                            String title,
                            String firstNames,
                            String lastName,
                            Integer genderConceptId,
                            String nhsNumber,
                            Date dateOfBirth,
                            Date dateOfDeath,
                            Long currentAddressId,
                            Integer ethnicCodeConceptId,
                            Long registeredPracticeId) throws Exception {

        super.printRecord(convertBoolean(false),
                "" + subscriberId.getSubscriberId(),
                "" + organizationId,
                title,
                firstNames,
                lastName,
                convertInt(genderConceptId),
                nhsNumber,
                convertDate(dateOfBirth),
                convertDate(dateOfDeath),
                convertLong(currentAddressId),
                convertInt(ethnicCodeConceptId),
                convertLong(registeredPracticeId));
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[]{
                "is_delete",
                "id",
                "organization_id",
                "title",
                "first_names",
                "last_name",
                "gender_concept_id",
                "nhs_number",
                "date_of_birth",
                "date_of_death",
                "current_address_id",
                "ethnic_code_concept_id",
                "registered_practice_organization_id"
        };
    }

    @Override
    public SubscriberTableId getTableId() {
        return SubscriberTableId.PERSON;
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[] {
                Byte.TYPE,
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
                Long.TYPE,
        };
    }

}
