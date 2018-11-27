package org.endeavourhealth.transform.common;

import com.google.common.base.Strings;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.core.database.dal.audit.models.PublishedFileColumn;
import org.endeavourhealth.core.database.dal.audit.models.PublishedFileType;
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

public abstract class AbstractFixedParser implements AutoCloseable, ParserI {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractFixedParser.class);

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
    //private int fieldPositionAdjuster = 1; // Assumes first field is defined as starting in position 1
    //private final SourceFileMappingDalI dal = DalProvider.factorySourceFileMappingDal();

    //audit data
    private Integer fileAuditId = null;
    //private long[] cellAuditIds = new long[10000]; //default to 10k audits
    private Integer numLines = null; //only set if we audit the file
    //private CsvAuditorCallbackI auditorCallback = null; //allows selective auditing of records

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

        //work out the fields and validate they make sense
        Set<String> fieldSet = new HashSet<>();
        int lastEndPos = 0;

        List<FixedParserField> fields = getFieldList(version);
        for (FixedParserField field: fields) {

            String fieldName = field.getName();
            if (fieldSet.contains(fieldName)) {
                throw new Exception("Duplicate name " + fieldName + " in " + getClass().getName());
            }
            fieldSet.add(fieldName);

            int newEndPos = field.getFieldPosition() + field.getFieldlength();
            if (newEndPos <= lastEndPos) {
                throw new Exception("Field " + fieldName + " starts before end of previous field");
            }

            addFieldList(field);
        }
    }


    protected abstract boolean isFileAudited();
    protected abstract boolean skipFirstRow();

    @Override
    public List<String> getColumnHeaders() {
        List<String> ret = new ArrayList<>();
        for (String header: fieldList.keySet()) {
            ret.add(header);
        }
        return ret;
    }

    public List<FixedParserField> getFieldList() {
        return getFieldList(version);
    }

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

    /*@Override
    public void setAuditorCallback(CsvAuditorCallbackI auditorCallback) {
        this.auditorCallback = auditorCallback;
    }*/

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

            //create (or find if re-processing) an audit entry for this file
            ensureFileAudited();

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
            throw new RuntimeException("Field position is one-based integer, so first column should start at position ONE");
        }

        //set the column index on the field
        int size = fieldList.size();
        field.setColumnIndex(size);

        fieldList.put(field.getName(), field);
    }

    protected String getString(String column) {
        FixedParserField field = fieldList.get(column);
        if (field != null) {
            return getFieldValue(this.curentLine, field);
        } else {
            return null;
        }
    }

    protected Integer getInt(String column) {
        String s = getString(column);
        if (s == null || s.length() == 0) {
            return null;
        }
        return Integer.valueOf(s);
    }

    protected Date getDateTime(String column) throws TransformException {
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

    protected Date getDate(String column) throws TransformException {
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

    protected Date getTime(String column) throws TransformException {
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

    protected Date getDateTime(String dateColumn, String timeColumn) throws TransformException {
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

    protected Long getLong(String column) {
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

        PublishedFileType publishedFileType = new PublishedFileType();
        publishedFileType.setFileType(getClass().getSimpleName());

        for (FixedParserField field: getFieldList()) {
            PublishedFileColumn publishedFileColumn = new PublishedFileColumn();
            publishedFileColumn.setColumnName(field.getName());
            publishedFileColumn.setFixedColumnStart(field.getFieldPosition());
            publishedFileColumn.setFixedColumnLength(field.getFieldlength());
            publishedFileType.getColumns().add(publishedFileColumn);
        }

        this.fileAuditId = PublishedFileAuditHelper.auditPublishedFileRecord(this, skipFirstRow(), publishedFileType);
    }

    /*private void ensureFileAudited() throws Exception {

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

        List<String> headersList = getColumnHeaders();

        //String fileTypeDesc = getFileTypeDescription();
        String fileTypeDesc = getClass().getSimpleName();
        int fileTypeId = dal.findOrCreateFileTypeId(serviceId, fileTypeDesc, headersList);

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
            AuditRowTask nextTask = new AuditRowTask(haveProcessedFileBefore);

            while (nextRecord()) {

                if (this.currentLineNumber % 50000 == 0) {
                    LOG.trace("Auditing Line " + currentLineNumber + " of " + filePath);
                }

                //check if we want to audit this record
                if (auditorCallback != null
                        && !auditorCallback.shouldAuditRecord(this)) {
                    continue;
                }

                String[] values = new String[headersList.size()];

                for (int i=0; i<headersList.size(); i++) {
                    String header = headersList.get(i);
                    FixedParserField field = fieldList.get(header);

                    String value = getFieldValue(this.curentLine, field, fieldPositionAdjuster);
                    values[i] = value;
                }

                SourceFileRecord fileRecord = new SourceFileRecord();
                fileRecord.setSourceFileId(fileAuditId);
                fileRecord.setSourceLocation("" + this.currentLineNumber);
                fileRecord.setValues(values);

                nextTask.addRecord(fileRecord);
                if (nextTask.isFull()) {
                    List<ThreadPoolError> errors = threadPool.submit(nextTask);
                    handleErrors(errors);
                    nextTask = new AuditRowTask(haveProcessedFileBefore);
                }
            }

            if (!nextTask.isEmpty()) {
                List<ThreadPoolError> errors = threadPool.submit(nextTask);
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
        throw new TransformException("", exception);
    }*/

    /**
     * callback function when the thread tasks have found/created an audit ID for a row,
     * purposely synchronized so that there's no loss when the array is grown
     */
    /*@Override
    public synchronized void setRowAuditId(int recordNumber, Long rowAuditId) {

        //we only start the array at 10k entries, so grow if necessary by 150% each time
        if (recordNumber >= cellAuditIds.length) {

            int nextRowCount = (int)((double)cellAuditIds.length * 1.5d);

            long[] tmp = new long[nextRowCount];
            System.arraycopy(cellAuditIds, 0, tmp, 0, cellAuditIds.length);
            this.cellAuditIds = tmp;
        }

        cellAuditIds[recordNumber] = rowAuditId;

        if (numLines == null) {
            numLines = new Integer(recordNumber);
        } else {
            numLines = new Integer(Math.max(recordNumber, numLines.intValue()));
        }
    }*/

    private String getFieldValue(String line, FixedParserField field) {
        int start = field.getFieldPosition() - 1; //field position is one-based, so minus one to get index
        int end = start + field.getFieldlength();

        //if the last field is empty, some files don't pad it out with whitespace (e.g. Barts SUS Outpatient file)
        //so we need to handle this safely and just extract up to the end of the line.
        if (end > line.length()
                && field.getColumnIndex() + 1 == fieldList.size()) {
            end = line.length();
        }

        String ret = line.substring(start, end);
        ret = ret.trim();
        return ret;
    }

    @Override
    public CsvCell getCell(String column) {

        FixedParserField field = fieldList.get(column);
        if (field == null) {
            throw new IllegalArgumentException("No such field as [" + field + "]");
        }
        String value = getFieldValue(this.curentLine, field);

        //long rowAuditId = getSourceFileRecordIdForCurrentRow();
        int colIndex = field.getColumnIndex();

        if (fileAuditId != null) {
            return new CsvCell(fileAuditId.intValue(), currentLineNumber, colIndex, value, this);
        } else {
            return new CsvCell(-1, -1, colIndex, value, this);
        }
        //return new CsvCell(rowAuditId, colIndex, value, this);
    }

    /*public long getSourceFileRecordIdForCurrentRow() {
        if (fileAuditId != null) {
            return cellAuditIds[currentLineNumber];

        } else {
            //if the file isn't audited, return -1, so it's compatible with CsvCell which uses a primative long
            return -1;
        }
    }*/

    /*class AuditRowTask implements Callable {

        private boolean haveProcessedFileBefore = false;
        private List<SourceFileRecord> records = new ArrayList<>();
        private boolean full;
        private boolean empty;

        public AuditRowTask(boolean haveProcessedFileBefore) {
            this.haveProcessedFileBefore = haveProcessedFileBefore;
        }

        @Override
        public Object call() throws Exception {

            try {

                //if we've done this file before, re-load the past audit
                List<SourceFileRecord> recordsToInsert = null;

                if (haveProcessedFileBefore) {
                    recordsToInsert = new ArrayList<>();

                    for (SourceFileRecord record : records) {
                        int lineNumber = Integer.parseInt(record.getSourceLocation());
                        Long existingId = dal.findRecordAuditIdForRow(serviceId, fileAuditId, lineNumber);
                        if (existingId != null) {
                            long rowAuditId = existingId.longValue();
                            setRowAuditId(lineNumber, rowAuditId);

                        } else {
                            recordsToInsert.add(record);
                        }
                    }
                } else {
                    recordsToInsert = records;
                }

                if (!recordsToInsert.isEmpty()) {
                    dal.auditFileRows(serviceId, recordsToInsert);

                    //the above call will have set the IDs in each of the record objects
                    for (SourceFileRecord record: recordsToInsert) {
                        Long rowAuditId = record.getId();
                        int lineNumber = Integer.parseInt(record.getSourceLocation());
                        setRowAuditId(lineNumber, rowAuditId);
                    }
                }

                return null;
            } catch (Exception ex) {

                String err = "Exception auditing rows ";
                for (SourceFileRecord record : records) {
                    err += record.getSourceLocation();
                    err += ", ";
                }
                LOG.error(err, ex);
                throw ex;
            }
        }

        public void addRecord(SourceFileRecord fileRecord) {
            this.records.add(fileRecord);
        }

        public boolean isFull() {
            return this.records.size() >= TransformConfig.instance().getResourceSaveBatchSize();
        }

        public boolean isEmpty() {
            return this.records.isEmpty();
        }
    }*/
}
