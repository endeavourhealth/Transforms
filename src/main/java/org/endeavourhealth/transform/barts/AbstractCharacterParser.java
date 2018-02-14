package org.endeavourhealth.transform.barts;

import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherTransform.SourceFileMappingDalI;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.CsvCurrentState;
import org.endeavourhealth.transform.common.exceptions.FileFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.UUID;

public abstract class AbstractCharacterParser implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractCharacterParser.class);

    private final UUID serviceId;
    private final UUID systemId;
    private final UUID exchangeId;
    private final String version;
    private final String delimiter;
    private final String filePath;
    private final DateFormat dateFormat;
    private final DateFormat timeFormat;
    private final DateFormat dateTimeFormat;
    private String curentLine;
    private String[] curentLineSplit;
    private BufferedReader br;
    private int currentLineNumber;
    private ArrayList<String> fieldList = new ArrayList<>();
    private int fieldPositionAdjuster = 1; // Assumes first field is defined as starting in position 1

    //audit data
    private final SourceFileMappingDalI dal = DalProvider.factorySourceFileMappingDal();
    private Integer fileAuditId = null;
    private boolean haveProcessedFileBefore = false;
    private long[] cellAuditIds = null;

    public AbstractCharacterParser(UUID serviceId, UUID systemId, UUID exchangeId,
                                   String version, String filePath, String delimiter,
                                   boolean openParser, String dateFormat, String timeFormat) throws Exception {

        this.serviceId = serviceId;
        this.systemId = systemId;
        this.exchangeId = exchangeId;
        this.version = version;
        this.filePath = filePath;
        this.dateFormat = new SimpleDateFormat(dateFormat);
        this.timeFormat = new SimpleDateFormat(timeFormat);
        this.dateTimeFormat = new SimpleDateFormat(dateFormat + " " + timeFormat);
        this.delimiter = delimiter;

        if (openParser) {
            open();
        }
    }

    private void open() throws Exception {
        try {
            InputStreamReader reader = FileHelper.readFileReaderFromSharedStorage(filePath);
            this.br = new BufferedReader(reader);
            currentLineNumber = 0;

            //create (or find if re-processing) an audit entry for this file
            ensureFileAudited();

        } catch (Exception e) {
            //if we get any exception thrown during the constructor, make sure to close the reader
            close();
            throw e;
        }
    }

    private void ensureFileAudited() throws Exception {
        if (this.fileAuditId != null) {
            return;
        }

        int fileTypeId = dal.findOrCreateFileTypeId(serviceId, getFileTypeDescription(), fieldList);

        Integer existingId = dal.findFileAudit(serviceId, systemId, exchangeId, fileTypeId, filePath);
        if (existingId != null) {
            this.fileAuditId = existingId.intValue();

            //if we've already been through this file at some point, then we need to re-load the audit IDs
            //for each of the cells in this file, so set this to true
            this.haveProcessedFileBefore = true;

        } else {
            this.fileAuditId = dal.auditFile(serviceId, systemId, exchangeId, fileTypeId, filePath);
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
            curentLineSplit = curentLine.split(delimiter);

            ensureRowAudited();

            return true;
        } else {
            //only log out we "completed" the file if we read any rows from it
            if (currentLineNumber > 1) {
                LOG.info("Completed file " + filePath);
            }
            curentLine = null;
            curentLineSplit = null;
            //automatically close the parser once we reach the end, to cut down on memory use
            close();
            return false;
        }
    }

    private void ensureRowAudited() throws Exception {

        //ensure our array has enough capaticy
        if (cellAuditIds == null
                || currentLineNumber >= cellAuditIds.length) {
            growCellAuditIdsArray();
        }

        long rowAuditId = cellAuditIds[currentLineNumber];

        //because it's a 2D array of primatives, check for zero rather than null
        //if (rowAuditIds != null) {
        if (rowAuditId > 0) {
            return;
        }

        //if we've done this file before, re-load the past audit
        if (this.haveProcessedFileBefore) {
            Long existingId = dal.findRecordAuditIdForRow(serviceId, fileAuditId, currentLineNumber);
            if (existingId != null) {
                rowAuditId = existingId.longValue();
            }
        }

        //if we still don't have audits, create new ones
        //because it's a 2D array of primatives, check for zero rather than null
        if (rowAuditId == 0) {
            rowAuditId = dal.auditFileRow(serviceId, curentLineSplit, currentLineNumber, fileAuditId);
        }

        cellAuditIds[currentLineNumber] = rowAuditId;
    }


    private void growCellAuditIdsArray() {

        //start at 10k in the array and grow by 50% each time
        if (cellAuditIds == null) {
            this.cellAuditIds = new long[10000];

        } else {

            int nextRowCount = (int)((double)cellAuditIds.length * 2d);

            long[] tmp = new long[nextRowCount];
            System.arraycopy(cellAuditIds, 0, tmp, 0, cellAuditIds.length);
            this.cellAuditIds = tmp;
        }
    }

    public void addFieldList(FixedParserField field) {
        fieldList.add(field.getName());
    }

    public void addFieldList(String field) {
        fieldList.add(field);
    }

    // This method is here to introduce a generic name accorss CSV and fixed-width
    public String[] getHeaders(String version) {
        return (String[]) fieldList.toArray();
    }

    protected String[] getCsvHeaders(String version) {
        return getHeaders(version);
    }

    public String getString(String column) throws FileFormatException {
        int fieldPos = fieldList.indexOf(column);
        if (fieldPos >= 0) {
            //LOG.trace("Field found - pos:" + fieldPos);
            String content = curentLineSplit[fieldPos].trim();
            //LOG.trace("Field content>" + content + "<");
            return content;
        } else {
            throw new FileFormatException(filePath, "Field not defined [" + column + "]");
        }
    }

    public Integer getInt(String column) throws FileFormatException {
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

    public Date getDate (String column) throws TransformException {
        String dt = getString(column);
        return parseDate(dt);
    }

    public Date parseDate (String date) throws TransformException {
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

    public Long getLong(String column) throws FileFormatException {
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


    public CsvCell getCell(String column) throws FileFormatException {

        String value;

        int colIndex = fieldList.indexOf(column);
        if (colIndex >= 0) {
            value = curentLineSplit[colIndex];

        } else {
            throw new FileFormatException(filePath, "Field not defined [" + column + "]");
        }

        //to save messy handling of non-empty but "empty" strings, trim whitespace of any non-null value
        if (value != null) {
            value = value.trim();
        }

        long rowAuditId = cellAuditIds[currentLineNumber];

        return new CsvCell(rowAuditId, colIndex, value, dateFormat, timeFormat);
    }

    protected abstract String getFileTypeDescription();
}
