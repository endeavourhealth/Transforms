package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;

import java.util.Date;

public class RegistrationStatusHistory extends AbstractTargetTable  {

    public RegistrationStatusHistory(CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(csvFormat, dateFormat, timeFormat);
    }

    public void writeUpsert(SubscriberId subscriberId,
                            long organisationId,
                            long patientId,
                            long personId,
                            long episodeOfCareId,
                            Integer registrationStatusId,
                            Date start,
                            Date end) throws Exception {

        super.printRecord(convertBoolean(false),
                "" + subscriberId.getSubscriberId(),
                "" + organisationId,
                "" + patientId,
                "" + personId,
                "" + episodeOfCareId,
                convertInt(registrationStatusId),
                convertDateTime(start),
                convertDateTime(end));
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[] {
                Byte.class,
                Long.TYPE,
                Long.TYPE,
                Long.TYPE,
                Long.TYPE,
                Long.TYPE,
                Integer.class,
                Date.class,
                Date.class
        };
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[] {
                "is_delete",
                "id",
                "organization_id",
                "patient_id",
                "person_id",
                "episode_of_care_id",
                "registration_status_concept_id",
                "start_date",
                "end_date"
        };
    }

    @Override
    public SubscriberTableId getTableId() {
        return SubscriberTableId.REGISTRATION_STATUS_HISTORY;
    }

}
