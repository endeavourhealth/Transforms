package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;

import java.util.Date;

public class EventLog extends AbstractTargetTable {

    public final static byte EVENT_LOG_INSERT = 0;
    public final static byte EVENT_LOG_UPDATE = 1;
    public final static byte EVENT_LOG_DELETE = 2;

    public EventLog(CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(csvFormat, dateFormat, timeFormat);
    }


    public void writeUpsert(long id,
                            Date entryDate,
                            byte entryMode,
                            byte tableId) throws Exception {

        super.printRecord(
                "" + id,
                convertDate(entryDate),
                "" + entryMode,
                "" + tableId);
    }

    @Override
    public Class[] getColumnTypes() {
        return new Class[]{
                Long.TYPE,
                Date.class,
                Byte.TYPE,
                Byte.TYPE
        };
    }

    @Override
    public String[] getCsvHeaders() {
        return new String[] {
                "id",
                "entry_date",
                "entry_mode",
                "table_id"
        };
    }

    @Override
    public SubscriberTableId getTableId() {
        return SubscriberTableId.EVENT_LOG;
    }
}
