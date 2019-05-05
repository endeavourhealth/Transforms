package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;

public class PseudoId extends AbstractTargetTable {

    public PseudoId(CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(csvFormat, dateFormat, timeFormat);
    }

    public void writeDelete(SubscriberId subscriberId) throws Exception {

        super.printRecord(
                "" + EventLog.EVENT_LOG_DELETE,
                "" + subscriberId.getSubscriberId()
        );
    }


    public void writeUpsert(SubscriberId subscriberId, long patientId, String saltKeyName, String pseudoId) throws Exception {

        super.printRecord(
                getEventTypeDesc(subscriberId),
                "" + subscriberId.getSubscriberId(),
                convertLong(patientId),
                saltKeyName,
                pseudoId
        );
    }


    @Override
    public Class[] getColumnTypes() {
        return new Class[] {
                Byte.TYPE,
                Long.TYPE,
                String.class,
                String.class,
                String.class
        };
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[] {
                "save_mode",
                "source_skid",
                "target_salt_key_name",
                "target_skid"
        };
    }

    @Override
    public SubscriberTableId getTableId() {
        return SubscriberTableId.PSEUDO_ID;
    }
}
