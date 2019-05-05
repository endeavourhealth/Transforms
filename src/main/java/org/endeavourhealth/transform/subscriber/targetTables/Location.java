package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;

public class Location extends AbstractTargetTable {

    public Location(CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(csvFormat, dateFormat, timeFormat);
    }

    public void writeDelete(SubscriberId subscriberId) throws Exception {

        super.printRecord("" + EventLog.EVENT_LOG_DELETE,
                "" + subscriberId.getSubscriberId());
    }

    public void writeUpsert(SubscriberId subscriberId,
                            String name,
                            String typeCode,
                            String typeDesc,
                            String postcode,
                            Long managingOrganisationId) throws Exception {

        super.printRecord(getEventTypeDesc(subscriberId),
                "" + subscriberId.getSubscriberId(),
                name,
                typeCode,
                typeDesc,
                postcode,
                convertLong(managingOrganisationId));
    }


    @Override
    public Class[] getColumnTypes() {
        return new Class[] {
                Byte.TYPE,
                Long.TYPE,
                String.class,
                String.class,
                String.class,
                String.class,
                Long.class
        };
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[] {
                "save_mode",
                "id",
                "name",
                "type_code",
                "type_desc",
                "postcode",
                "managing_organization_id"
        };
    }

    @Override
    public SubscriberTableId getTableId() {
        return SubscriberTableId.LOCATION;
    }

}
