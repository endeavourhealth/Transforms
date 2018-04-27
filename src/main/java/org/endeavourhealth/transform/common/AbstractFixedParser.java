package org.endeavourhealth.transform.common;

import com.google.common.base.Strings;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherTransform.SourceFileMappingDalI;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.exceptions.FileFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;

public abstract class AbstractFixedParser implements AutoCloseable, ParserI {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractFixedParser.class);

    private final SourceFileMappingDalI dal = DalProvider.factorySourceFileMappingDal();

    private final UUID serviceId;
    private final UUID systemId;
    private final UUID exchangeId;
    private final String version;
    private final String filePath;
    private final DateFormat dateFormat;
    private final DateFormat timeFormat;
    private final DateFormat dateTimeFormat;
    private String curentLine;
    private BufferedReader br;
    private int currentLineNumber;
    private LinkedHashMap<String, FixedParserField> fieldList = new LinkedHashMap<String, FixedParserField>();
    private int fieldPositionAdjuster = 1; // Assumes first field is defined as starting in position 1

    //audit data
    private Integer fileAuditId = null;
    private long[] cellAuditIds = new long[10000]; //default to 10k audits
    private Integer numLines = null; //only set if we audit the file


    public AbstractFixedParser(UUID serviceId, UUID systemId, UUID exchangeId,
                               String version, String filePath, String dateFormat, String timeFormat) throws Exception {

        this.serviceId = serviceId;
        this.systemId = systemId;
        this.exchangeId = exchangeId;
        this.version = version;
        this.filePath = filePath;
        this.dateFormat = new SimpleDateFormat(dateFormat);
        this.timeFormat = new SimpleDateFormat(timeFormat);
        this.dateTimeFormat = new SimpleDateFormat(dateFormat + " " + timeFormat);

        List<FixedParserField> fields = getFieldList(version);
        for (FixedParserField field: fields) {
            addFieldList(field);
        }

        //create (or find if re-processing) an audit entry for this file
        ensureFileAudited();
    }

    protected abstract String getFileTypeDescription();
    protected abstract boolean isFileAudited();
    protected abstract boolean skipFirstRow();

    @Override
    public UUID getServiceId() {
        return serviceId;
    }

    @Override
    public UUID getSystemId() {
        return systemId;
    }

    @Override
    public UUID getExchangeId() {
        return exchangeId;
    }

    @Override
    public DateFormat getDateFormat() {
        return dateFormat;
    }

    @Override
    public DateFormat getTimeFormat() {
        return timeFormat;
    }

    @Override
    public DateFormat getDateTimeFormat() {
        return dateTimeFormat;
    }

    private void open(String action) throws Exception {

        if (numLines == null) {
            LOG.info(action + " " + filePath);
        } else {
            LOG.info(action + " " + filePath + " (" + numLines + " lines)");
        }

        try {
            InputStreamReader fr = FileHelper.readFileReaderFromSharedStorage(filePath);
            this.br = new BufferedReader(fr);
            currentLineNumber = 0;

        } catch (Exception e) {
            //if we get any exception thrown during the constructor, make sure to close the reader
            close();
            throw e;
        }

        //some fixed width files contain a header row (e.g. Cerner prob file) and some don't (e.g. Cerner SUS files)
        if (skipFirstRow()) {
            nextRecord();
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
            open("Starting");
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

    protected abstract List<FixedParserField> getFieldList(String version);

    private void addFieldList(FixedParserField field) {
        if (field.getFieldPosition() == 0) {
            // Calculate field position adjuster - first field can be configured from position 1 or zero but String.subString() starts at position 0
            fieldPositionAdjuster = 0;
        }

        //set the column index on the field
        int size = fieldList.size();
        field.setColumnIndex(size);

        fieldList.put(field.getName(), field);
    }

    protected List<String> getHeaders() {
        List<String> ret = new ArrayList<>();
        for (String header: fieldList.keySet()) {
            ret.add(header);
        }
        return ret;
    }

    public String getString(String column) {
        FixedParserField field = fieldList.get(column);
        if (field != null) {
            return getFieldValue(this.curentLine, field, fieldPositionAdjuster);
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
        if (date != null) {
            date = date.trim();
        }

        if (Strings.isNullOrEmpty(date)) {
            return null;
        }

        try {
            return dateFormat.parse(date);
        } catch (ParseException pe) {
            throw new FileFormatException(filePath, "Invalid date format [" + date + "]", pe);
        }
    }
    /*public Date parseDate(String date) throws TransformException {
        Date ret = null;
        if (date != null && date.length() > 0) {
            try {
                ret = dateFormat.parse(date);
            } catch (ParseException pe) {
                throw new FileFormatException(filePath, "Invalid date format [" + date + "]", pe);
            }
        }
        return ret;
    }*/

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


    /**
     * when we open this file, this function is called to ensure the content is fully audited. If not, it will use
     * a thread pool to iterate through the file and ensure every row is audited
     */
    private void ensureFileAudited() throws Exception {

        //if this file doesn't need auditing, just return out
        if (!isFileAudited()) {
            return;
        }

        //to work out the Emis CSV version, we create parsers but don't use them to process any records, so
        //detect this by the null service ID and just return out
        if (this.serviceId == null) {
            return;
        }

        //if we've already audited this file, then just return out
        if (this.fileAuditId != null) {
            return;
        }

        //start reading the file
        open("Auditing");

        List<String> headersList = getHeaders();

        int fileTypeId = dal.findOrCreateFileTypeId(serviceId, getFileTypeDescription(), headersList);

        boolean haveProcessedFileBefore = false;

        Integer existingId = dal.findFileAudit(serviceId, systemId, exchangeId, fileTypeId, filePath);
        if (existingId != null) {
            this.fileAuditId = existingId.intValue();

            //if we've already been through this file at some point, then we need to re-load the audit IDs
            //for each of the cells in this file, so set this to true
            haveProcessedFileBefore = true;

        } else {
            this.fileAuditId = dal.auditFile(serviceId, systemId, exchangeId, fileTypeId, filePath);
        }

        int threadPoolSize = ConnectionManager.getPublisherCommonConnectionPoolMaxSize();
        ThreadPool threadPool = new ThreadPool(threadPoolSize, 5000);

        try {
            while (nextRecord()) {

                if (this.currentLineNumber % 50000 == 0) {
                    LOG.trace("Auditing Line " + currentLineNumber + " of " + filePath);
                }

                String[] values = new String[headersList.size()];

                for (int i=0; i<headersList.size(); i++) {
                    String header = headersList.get(i);
                    FixedParserField field = fieldList.get(header);

                    String value = getFieldValue(this.curentLine, field, fieldPositionAdjuster);
                    values[i] = value;
                }

                AuditRowTask task = new AuditRowTask(this.currentLineNumber, values, haveProcessedFileBefore);
                List<ThreadPoolError> errors = threadPool.submit(task);
                handleErrors(errors);
            }

        } finally {
            //close the parser once we reach the end, to cut down on memory use
            close();

            List<ThreadPoolError> errors = threadPool.waitAndStop();
            handleErrors(errors);
        }
    }

    private static void handleErrors(List<ThreadPoolError> errors) throws Exception {
        if (errors == null || errors.isEmpty()) {
            return;
        }

        //if we've had multiple errors, just throw the first one, since they'll most-likely be the same
        ThreadPoolError first = errors.get(0);
        AuditRowTask callable = (AuditRowTask) first.getCallable();
        Throwable exception = first.getException();
        CsvCurrentState parserState = callable.getParserState();
        throw new TransformException(parserState.toString(), exception);
    }

    /**
     * callback function when the thread tasks have found/created an audit ID for a row,
     * purposely synchronized so that there's no loss when the array is grown
     */
    private synchronized void setRowAuditId(int lineNumber, long rowAuditId) {

        //we only start the array at 10k entries, so grow if necessary by 150% each time
        if (lineNumber >= cellAuditIds.length) {

            int nextRowCount = (int)((double)cellAuditIds.length * 1.5d);

            long[] tmp = new long[nextRowCount];
            System.arraycopy(cellAuditIds, 0, tmp, 0, cellAuditIds.length);
            this.cellAuditIds = tmp;
        }

        cellAuditIds[lineNumber] = rowAuditId;

        if (numLines == null) {
            numLines = new Integer(lineNumber);
        } else {
            numLines = new Integer(Math.max(lineNumber, numLines.intValue()));
        }
    }

    private static String getFieldValue(String line, FixedParserField field, int offset) {
        int start = field.getFieldPosition() - offset;
        int end = (field.getFieldPosition() - offset) + field.getFieldlength();
        String ret = line.substring(start, end);
        ret = ret.trim();
        return ret;
    }

    public CsvCell getCell(String column) {

        FixedParserField field = fieldList.get(column);
        if (field == null) {
            throw new IllegalArgumentException("No such field as [" + field + "]");
        }
        String value = getFieldValue(this.curentLine, field, fieldPositionAdjuster);

        long rowAuditId = getSourceFileRecordIdForCurrentRow();
        int colIndex = field.getColumnIndex();

        return new CsvCell(rowAuditId, colIndex, value, this);
    }

    public long getSourceFileRecordIdForCurrentRow() {
        if (isFileAudited()) {
            return cellAuditIds[currentLineNumber];

        } else {
            //if the file isn't audited, return -1, so it's compatible with CsvCell which uses a primative long
            return -1;
        }
    }


    class AuditRowTask implements Callable {

        private int lineNumber;
        private String[] values;
        private boolean haveProcessedFileBefore;
        private CsvCurrentState parserState;

        public AuditRowTask(int lineNumber, String[] values, boolean haveProcessedFileBefore) {
            this.lineNumber = lineNumber;
            this.values = values;
            this.haveProcessedFileBefore = haveProcessedFileBefore;
        }


        public CsvCurrentState getParserState() {
            return new CsvCurrentState(filePath, lineNumber);
        }

        @Override
        public Object call() throws Exception {

            try {

                long rowAuditId = -1;

                //if we've done this file before, re-load the past audit
                if (haveProcessedFileBefore) {
                    Long existingId = dal.findRecordAuditIdForRow(serviceId, fileAuditId, lineNumber);
                    if (existingId != null) {
                        rowAuditId = existingId.longValue();
                    }
                }

                //if we still don't have an audit for this row, create a new one
                if (rowAuditId == -1) {
                    rowAuditId = dal.auditFileRow(serviceId, values, lineNumber, fileAuditId);
                }

                //use the callback function to set it in our array
                setRowAuditId(lineNumber, rowAuditId);

                return null;
            } catch (Exception ex) {
                LOG.error("Exception auditing row " + lineNumber, ex);
                throw ex;
            }
        }

    }
}
