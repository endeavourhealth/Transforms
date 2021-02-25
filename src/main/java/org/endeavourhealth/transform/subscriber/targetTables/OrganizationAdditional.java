package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;

public class OrganizationAdditional extends AbstractTargetTable {

    public OrganizationAdditional(CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
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
                String.class,
		        String.class,
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
                "json_value",
		        "value",
                "name"
        };
    }

    @Override
    public SubscriberTableId getTableId() {
        return SubscriberTableId.ORGANIZATION_ADDITIONAL;
    }

    public void writeUpsert(SubscriberId id,
                            Integer propertyId,
                            Integer valueId,
                            String jsonValue,
			                String Value,
                            String name) throws Exception {

        //SD-382 - getting nulls in the property_id field for _additional tables
        if (propertyId == null) {
            throw new Exception("Null propertyId value for organization_additional record for organization " + id);
        }

        super.printRecord(convertBoolean(false),
                "" + id.getSubscriberId(),
                convertInt(propertyId),
                convertInt(valueId),
                jsonValue,
		        Value,
                name);
    }
}
