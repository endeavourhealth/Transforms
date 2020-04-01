package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;

public class Practitioner extends AbstractTargetTable {

    public Practitioner(CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(csvFormat, dateFormat, timeFormat);
    }

    public void writeDelete(SubscriberId subscriberId) throws Exception {

        super.printRecord(convertBoolean(true),
                "" + subscriberId.getSubscriberId());
    }

    public void writeUpsert(SubscriberId subscriberId,
                            long organizaationId,
                            String name,
                            String roleCode,
                            String roleDesc,
                            String gmcCode) throws Exception {

        super.printRecord(convertBoolean(false),
                "" + subscriberId.getSubscriberId(),
                "" + organizaationId,
                name,
                roleCode,
                roleDesc,
                gmcCode);
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[] {
                "is_delete",
                "id",
                "organization_id",
                "name",
                "role_code",
                "role_desc",
                "gmc_code"
        };
    }

    @Override
    public SubscriberTableId getTableId() {
        return SubscriberTableId.PRACTITIONER;
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
                String.class
        };
    }
}
