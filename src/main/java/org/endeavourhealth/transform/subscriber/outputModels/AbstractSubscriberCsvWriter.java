package org.endeavourhealth.transform.subscriber.outputModels;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.transform.common.AbstractCsvWriter;

public abstract class AbstractSubscriberCsvWriter extends AbstractCsvWriter {

    public AbstractSubscriberCsvWriter(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
    }

    public abstract void writeDelete(long id) throws Exception;
    public abstract Class[] getColumnTypes();
    public abstract String[] getCsvHeaders();
}
