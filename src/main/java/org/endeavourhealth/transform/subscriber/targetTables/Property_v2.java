package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.joda.time.DateTime;

import java.math.BigDecimal;
import java.util.Date;


public class Property_v2 extends AbstractTargetTable {
    public Property_v2(CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(csvFormat, dateFormat, timeFormat);
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[]{
                Byte.TYPE, // thingy
                Long.TYPE,
                Long.TYPE,
                Long.TYPE
        };
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[]{
                "is_delete",
                "id",
                "location_id",
                "project_ralf_id"
        };
    }

    @Override
    public SubscriberTableId getTableId() {
        return SubscriberTableId.PROPERTY_V2;
    }

    public void writeDelete(SubscriberId subscriberId) throws Exception {
        super.printRecord(
                convertBoolean(true),
                "" + subscriberId.getSubscriberId()
        );
    }

    public void writeUpsert(//SubscriberId subscriberId,
                            Long property_id,
                            Long location_id,
                            Long project_ralf_id
    ) throws Exception {

        super.printRecord(
                convertBoolean(false),
                //"" + subscriberId.getSubscriberId(),
                "" + convertLong(property_id),
                convertLong(location_id),
                convertLong(project_ralf_id)
        );
    }
}

