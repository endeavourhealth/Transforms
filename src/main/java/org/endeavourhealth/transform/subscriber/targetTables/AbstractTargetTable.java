package org.endeavourhealth.transform.subscriber.targetTables;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.transform.common.AbstractCsvWriter;

public abstract class AbstractTargetTable extends AbstractCsvWriter {

    public AbstractTargetTable(CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(null, csvFormat, dateFormat, timeFormat);
    }

    @Override
    public String getFileName() {
        return getTableId().getName() + ".csv";
    }

    public abstract Class[] getColumnTypes();
    public abstract String[] getCsvHeaders();
    public abstract SubscriberTableId getTableId();

    /*protected String getEventTypeDesc(SubscriberId subscriberId) {
        if (subscriberId.getDtUpdatedPreviouslySent() == null) {
            return "" + EventLog.EVENT_LOG_INSERT;

        } else {
            return "" + EventLog.EVENT_LOG_UPDATE;
        }
    }*/
}
