package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.joda.time.DateTime;

import java.math.BigDecimal;
import java.util.Date;


public class OrganizationContact_v2 extends AbstractTargetTable {
    public OrganizationContact_v2(CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(csvFormat, dateFormat, timeFormat);
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[]{
                Byte.TYPE, // thingy
                Long.TYPE,
                Long.TYPE,
                String.class,
                String.class
        };
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[]{
                "is_delete",
                "id",
                "organization_id",
                "contact_type",
                "value"
        };
    }

    @Override
    public SubscriberTableId getTableId() {
        return SubscriberTableId.ORGANIZATION_CONTACT_V2;
    }

    public void writeDelete(SubscriberId subscriberId) throws Exception {
        super.printRecord(
                convertBoolean(true),
                "" + subscriberId.getSubscriberId()
        );
    }

    public void writeUpsert(//SubscriberId subscriberId,
                            Long contact_id,
                            Long organization_id,
                            String contact_type,
                            String value
    ) throws Exception {

        super.printRecord(
                convertBoolean(false),
                //"" + subscriberId.getSubscriberId(),
                "" + convertLong(contact_id),
                convertLong(organization_id),
                contact_type,
                value
        );
    }
}

