package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;

public class ObservationAdditional extends AbstractTargetTable {

    public ObservationAdditional(CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(csvFormat, dateFormat, timeFormat);
    }

    public void writeDelete(SubscriberId subscriberId) throws Exception {
        super.printRecord(convertBoolean(true),
                "" + subscriberId.getSubscriberId());
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[] {
                Byte.TYPE,
                Long.TYPE,
                Integer.class,
                Integer.class,
                String.class
        };
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[] {
                "is_delete",
                "id",
                "property_id",
                "value_id",
                "json_value"
        };
    }

    @Override
    public SubscriberTableId getTableId() {
        return SubscriberTableId.OBSERVATION_ADDITIONAL;
    }

    public void writeUpsert(SubscriberId id,
                            Integer propertyId,
                            Integer valueId,
                            String jsonValue) throws Exception {

        super.printRecord(convertBoolean(false),
                convertLong(id.getSubscriberId()),
                convertInt(propertyId),
                convertInt(valueId),
                jsonValue);
    }
}