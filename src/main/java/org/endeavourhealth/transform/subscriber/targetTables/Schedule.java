package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.transform.subscriber.SubscriberTransformHelper;

import java.util.Date;
import java.util.List;

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
                            String name,
                            SubscriberTransformHelper params) throws Exception {

        //get any configured excluded columns for this table
        List<String> excludedTableColumns
                = params.getSubscriberExcludedColumns(getTableId(), getPermittedColumnExclusions());
        if (excludedTableColumns != null) {
            //check each column set for exclusion and blank
            for (String column: excludedTableColumns) {
                switch (column) {
                    case "name" : name = null; break;
                    case "location" : location = null; break;
                    default : break;
                }
            }
        }

        super.printRecord(
                convertBoolean(false),
                "" + subscriberId.getSubscriberId(),
                "" + organisationId,
                convertLong(practitionerId),
                convertDateTime(startDate), //SD-498
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

    @Override
    public String[] getPermittedColumnExclusions() {
        return new String[] {
                "name",
                "location"
        };
    }
}
