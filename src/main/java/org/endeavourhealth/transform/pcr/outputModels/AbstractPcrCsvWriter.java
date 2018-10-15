package org.endeavourhealth.transform.pcr.outputModels;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.transform.common.AbstractCsvWriter;

public abstract class AbstractPcrCsvWriter extends AbstractCsvWriter {

    public AbstractPcrCsvWriter(String fileName, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(fileName, csvFormat, dateFormat, timeFormat);
    }

    public abstract void writeDelete(long id) throws Exception;
    public abstract Class[] getColumnTypes();
    public abstract String[] getCsvHeaders();
}
