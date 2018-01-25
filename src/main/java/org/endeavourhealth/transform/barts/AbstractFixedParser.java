package org.endeavourhealth.transform.barts;

import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.transform.common.CsvCurrentState;
import org.endeavourhealth.transform.common.exceptions.FileFormatException;
import org.endeavourhealth.core.exceptions.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;

public abstract class AbstractFixedParser implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractFixedParser.class);

    private final String version;
    private final String filePath;
    private final DateFormat dateFormat;
    private final DateFormat timeFormat;
    private final DateFormat dateTimeFormat;
    private String curentLine;
    private BufferedReader br;
    private long currentLineNumber;
    private LinkedHashMap<String, FixedParserField> fieldList = new LinkedHashMap<String, FixedParserField>();
    private int fieldPositionAdjuster = 1; // Assumes first field is defined as starting in position 1

    public AbstractFixedParser(String version, String filePath, boolean openParser, String dateFormat, String timeFormat) throws Exception {

        this.version = version;
        this.filePath = filePath;
        this.dateFormat = new SimpleDateFormat(dateFormat);
        this.timeFormat = new SimpleDateFormat(timeFormat);
        this.dateTimeFormat = new SimpleDateFormat(dateFormat + " " + timeFormat);

        if (openParser) {
            open();
        }
    }

    private void open() throws Exception {
        try {
            InputStreamReader fr = FileHelper.readFileReaderFromSharedStorage(filePath);
            this.br = new BufferedReader(fr);
            currentLineNumber = 0;

        } catch (Exception e) {
            //if we get any exception thrown during the constructor, make sure to close the reader
            close();
            throw e;
        }
    }

    public void close() throws IOException {
        if (br != null) {
            br.close();
            br = null;
        }
    }

    public boolean nextRecord() throws Exception {

        //we now only open the first set of parsers when starting a transform, so
        //need to check to open the subsequent ones
        if (br == null) {
            open();
        }

        curentLine = br.readLine();
        if (curentLine != null && curentLine.length() > 0) {
            currentLineNumber++;
            return true;
        } else {
            //only log out we "completed" the file if we read any rows from it
            if (currentLineNumber > 1) {
                LOG.info("Completed file {}", filePath);
            }
            curentLine = null;
            //automatically close the parser once we reach the end, to cut down on memory use
            close();
            return false;
        }
    }

    public void addFieldList(FixedParserField field) {
        if (field.getFieldPosition() == 0) {
            // Calculate field position adjuster - first field can be configured from position 1 or zero but String.subString() starts at position 0
            fieldPositionAdjuster = 0;
        }
        fieldList.put(field.getName(), field);
    }

    // This method is here to introduce a generic name accorss CSV and fixed-width
    public String[] getHeaders(String version) {
        return (String[]) fieldList.keySet().toArray();
    }

    protected String[] getCsvHeaders(String version) {
        return getHeaders(version);
    }

    public String getString(String column) {
        FixedParserField field = fieldList.get(column);
        if (field != null) {
            //LOG.trace("Field found:" + field.getName() + " start:" + field.getFieldPosition() + " Length:" + field.getFieldlength());
            String content = curentLine.substring(field.getFieldPosition() - fieldPositionAdjuster, (field.getFieldPosition() - fieldPositionAdjuster) + field.getFieldlength()).trim();
            //LOG.trace("Field content>" + content + "<");
            return content;
        } else {
            return null;
        }
    }

    public Integer getInt(String column) {
        String s = getString(column);
        if (s == null || s.length() == 0) {
            return null;
        }
        return Integer.valueOf(s);
    }

    public Date getDateTime(String column) throws TransformException {
        Date ret = null;
        String dt = getString(column);
        if (dt != null && dt.length() > 0) {
            try {
                ret = dateTimeFormat.parse(dt);
            } catch (ParseException pe) {
                throw new FileFormatException(filePath, "Invalid date format [" + dt + "]", pe);
            }
        }
        return ret;
    }

    public Date getDate(String column) throws TransformException {
        String dt = getString(column);
        return parseDate(dt);
    }

    public Date parseDate(String date) throws TransformException {
        Date ret = null;
        if (date != null && date.length() > 0) {
            try {
                ret = dateFormat.parse(date);
            } catch (ParseException pe) {
                throw new FileFormatException(filePath, "Invalid date format [" + date + "]", pe);
            }
        }
        return ret;
    }

    public Date getTime(String column) throws TransformException {
        Date ret = null;
        String dt = getString(column);
        if (dt != null && dt.length() > 0) {
            try {
                ret = timeFormat.parse(dt);
            } catch (ParseException pe) {
                throw new FileFormatException(filePath, "Invalid date format [" + dt + "]", pe);
            }
        }
        return ret;
    }

    public Date getDateTime(String dateColumn, String timeColumn) throws TransformException {
        Date d = getDate(dateColumn);
        Date t = getTime(timeColumn);
        if (d == null) {
            return null;
        } else {
            if (t == null) {
                return d;
            } else {
                return new Date(d.getTime() + t.getTime());
            }
        }
    }

    public Long getLong(String column) {
        String s = getString(column);
        if (s == null) {
            return null;
        }
        return Long.valueOf(s);
    }

    public CsvCurrentState getCurrentState() {
        return new CsvCurrentState(filePath, currentLineNumber);
    }

    public String getFilePath() {
        return filePath;
    }

}
