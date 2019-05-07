package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;

import java.util.Date;

public class Schedule extends AbstractTargetTable {

    public Schedule(CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(csvFormat, dateFormat, timeFormat);
    }

    public void writeDelete(SubscriberId subscriberId) throws Exception {

        super.printRecord(convertBoolean(true),
                "" + subscriberId.getSubscriberId());
    }
    public void writeUpsert(SubscriberId subscriberId,
                            long organisationId,
                            Long practitionerId,
                            Date startDate,
                            String type,
                            String location,
                            String name) throws Exception {

        super.printRecord(convertBoolean(false),
                "" + subscriberId.getSubscriberId(),
                "" + organisationId,
                convertLong(practitionerId),
                convertDate(startDate),
                type,
                location,
                name);
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[] {
                "is_delete",
                "id",
                "organization_id",
                "practitioner_id",
                "start_date",
                "type",
                "location",
                "name"
        };
    }

    @Override
    public SubscriberTableId getTableId() {
        return SubscriberTableId.SCHEDULE;
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[] {
                Byte.TYPE,
                Long.TYPE,
                Long.TYPE,
                Long.class,
                Date.class,
                String.class,
                String.class,
                String.class
        };
    }
}
