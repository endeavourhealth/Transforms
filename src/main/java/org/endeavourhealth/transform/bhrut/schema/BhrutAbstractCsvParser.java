package org.endeavourhealth.transform.bhrut.schema;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;

import java.nio.charset.Charset;
import java.util.UUID;

public abstract class BhrutAbstractCsvParser extends AbstractCsvParser {
    public BhrutAbstractCsvParser(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath, CSVFormat csvFormat, String dateFormat, String timeFormat) {
        super(serviceId, systemId, exchangeId, version, filePath, csvFormat, dateFormat, timeFormat);
    }

    public BhrutAbstractCsvParser(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath, CSVFormat csvFormat, String dateFormat, String timeFormat, Charset encoding) {
        super(serviceId, systemId, exchangeId, version, filePath, csvFormat, dateFormat, timeFormat, encoding);
    }


    public CsvCell getCell(String column) {
        CsvCell cell = super.getCell(column);
        String val ="";
        if (!cell.getString().isEmpty()) {
            val = cell.getString().replaceAll("^\"|\"$", "");
        }
        CsvCell ret = new CsvCell(cell.getPublishedFileId(),
                cell.getRecordNumber(),
                cell.getColIndex(),
                val,
                cell.getParentParser());
        return ret;
    }

}
