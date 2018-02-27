package org.endeavourhealth.transform.common;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.common.utility.ThreadPool;
import org.endeavourhealth.common.utility.ThreadPoolError;
import org.endeavourhealth.core.csv.CsvHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.publisherTransform.SourceFileMappingDalI;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.exceptions.FileFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;

public abstract class AbstractCsvParser implements AutoCloseable, ParserI {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractCsvParser.class);

    private final UUID serviceId;
    private final UUID systemId;
    private final UUID exchangeId;
    private final String version;
    private final String filePath;
    private final CSVFormat csvFormat;
    private final DateFormat dateFormat;
    private final DateFormat timeFormat;
    private final SourceFileMappingDalI dal = DalProvider.factorySourceFileMappingDal();

    private CSVParser csvReader = null;
    private Iterator<CSVRecord> csvIterator = null;
    private CSVRecord csvRecord = null;
    private int csvRecordLineNumber = -1;
    private Set<Long> recordNumbersToProcess = null;

    //audit data
    private Integer fileAuditId = null;
    private long[] cellAuditIds = new long[10000]; //default to 10k audits
    private Integer numLines = null; //only set if we audit the file

    public AbstractCsvParser(UUID serviceId, UUID systemId, UUID exchangeId,
                             String version, String filePath, CSVFormat csvFormat,
                             String dateFormat, String timeFormat) throws Exception {

        this.serviceId = serviceId;
        this.systemId = systemId;
        this.exchangeId = exchangeId;
        this.version = version;
        this.filePath = filePath;
        this.csvFormat = csvFormat;
        this.dateFormat = new SimpleDateFormat(dateFormat);
        this.timeFormat = new SimpleDateFormat(timeFormat);

        //create (or find if re-processing) an audit entry for this file
        ensureFileAudited();
    }


    private void open(String action) throws Exception {

        if (numLines == null) {
            LOG.info(action + " " + filePath);
        } else {
            LOG.info(action + " " + filePath + " (" + numLines + " lines)");
        }

        InputStreamReader isr = FileHelper.readFileReaderFromSharedStorage(filePath);
        this.csvReader = new CSVParser(isr, csvFormat);
        try {
            this.csvIterator = csvReader.iterator();

            String[] expectedHeaders = getCsvHeaders(version);
            CsvHelper.validateCsvHeaders(csvReader, filePath, expectedHeaders);

            csvRecordLineNumber = 0;

        } catch (Exception e) {
            //if we get any exception thrown during the constructor, make sure to close the reader
            close();
            throw e;
        }
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

        String[] headers = CsvHelper.getHeaderMapAsArray(this.csvReader);
        List<String> headersList = Arrays.asList(headers);

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

        //now audit the individual rows in the file, using a thread pool to audit rows in parallel
        Map<String, Integer> headerMap = csvReader.getHeaderMap();

        int threadPoolSize = ConnectionManager.getPublisherCommonConnectionPoolMaxSize();
        ThreadPool threadPool = new ThreadPool(threadPoolSize, 5000);

        try {
            while (advanceToNextRow()) { //this just iterates through the rows, but handles S3 errors and recovers

                if (this.csvRecordLineNumber % 50000 == 0) {
                    LOG.trace("Auditing Line " + csvRecordLineNumber + " of " + filePath);
                }

                String[] values = new String[headerMap.size()];

                for (String header: headerMap.keySet()) {
                    Integer colIndex = headerMap.get(header);
                    String value = csvRecord.get(colIndex);
                    values[colIndex.intValue()] = value;
                }

                AuditRowTask task = new AuditRowTask(this.csvRecordLineNumber, values, haveProcessedFileBefore);
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


    /*private void ensureFileAudited() throws Exception {
        if (this.fileAuditId != null) {
            return;
        }

        String[] headers = CsvHelper.getHeaderMapAsArray(this.csvReader);
        List<String> headersList = Arrays.asList(headers);

        int fileTypeId = dal.findOrCreateFileTypeId(serviceId, getFileTypeDescription(), headersList);

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

    private void ensureRowAudited() throws Exception {

        //ensure our array has enough capaticy
        if (cellAuditIds == null
                || csvRecordLineNumber >= cellAuditIds.length) {
            growCellAuditIdsArray();
        }

        long rowAuditId = cellAuditIds[csvRecordLineNumber];

        //because it's a 2D array of primatives, check for zero rather than null
        //if (rowAuditIds != null) {
        if (rowAuditId > 0) {
            return;
        }

        //if we've done this file before, re-load the past audit
        if (this.haveProcessedFileBefore) {
            Long existingId = dal.findRecordAuditIdForRow(serviceId, fileAuditId, csvRecordLineNumber);
            if (existingId != null) {
                rowAuditId = existingId.longValue();
            }
        }

        //if we still don't have audits, create new ones
        //because it's a 2D array of primatives, check for zero rather than null
        if (rowAuditId == 0) {

            Map<String, Integer> headers = csvReader.getHeaderMap();
            String[] values = new String[headers.size()];

            for (String header: headers.keySet()) {
                Integer colIndex = headers.get(header);
                String value = csvRecord.get(colIndex);
                values[colIndex.intValue()] = value;
            }

            rowAuditId = dal.auditFileRow(serviceId, values, csvRecordLineNumber, fileAuditId);
        }

        cellAuditIds[csvRecordLineNumber] = rowAuditId;
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
    }*/

    public void close() throws IOException {

        //only log out we "completed" the file if we read any rows from it
        if (csvRecordLineNumber >= 1) {
            LOG.info("Completed " + filePath);
        }

        if (csvReader != null) {
            csvReader.close();
            csvReader = null;
        }

        //may as well clear these as well
        this.csvIterator = null;
        this.csvRecord = null;
        this.csvRecordLineNumber = -1;
    }

    public List<String> testForValidVersions(List<String> possibleVersions) throws Exception {

        List<String> ret = new ArrayList<>();

        String firstChars = FileHelper.readFirstCharactersFromSharedStorage(filePath, 1000); //assuming we never have headers longer than 1000 bytes
        //LOG.trace("Read first 1000 chars from " + filePath);
        //LOG.trace(firstChars);

        StringReader stringReader = new StringReader(firstChars);
        CSVParser csvReader = new CSVParser(stringReader, csvFormat); //not assigning to class variable as this reader is just for this validation
        try {

            for (String possibleVersion: possibleVersions) {
                String[] expectedHeaders = getCsvHeaders(possibleVersion);
                try {
                    CsvHelper.validateCsvHeaders(csvReader, filePath, expectedHeaders);

                    //if we call the above and don't get an exception, the possible versio is valid for the headers found
                    ret.add(possibleVersion);
                    //LOG.trace("Valid version " + possibleVersion);

                } catch (IllegalArgumentException ex) {
                    //if we get this exception, the headers don't match the possible version
                    //LOG.trace("Not valid version " + possibleVersion);
                }
            }

        } finally {
            stringReader.close();
        }

        if (possibleVersions.isEmpty()) {
            LOG.error("Ruled out all possible versions because of file " + filePath);
        }

        return ret;
    }

    /*public void reset() throws Exception {

        //if we've opened the parser but only read the header, don't bother resetting
        if (getCurrentLineNumber() <= 1) {
            return;
        }

        close();
        open();
    }*/

    protected abstract String[] getCsvHeaders(String version);

    protected abstract String getFileTypeDescription();
    protected abstract boolean isFileAudited();

    public boolean nextRecord() throws Exception {

        //we now only open the first set of parsers when starting a transform, so
        //need to check to open the subsequent ones
        if (csvReader == null) {
            open("Starting");
        }

        //if the source file couldn't be found, the iterator will be null
        if (csvIterator == null) {
            return false;
        }

        while (advanceToNextRow()) {

            if (this.csvRecordLineNumber % 50000 == 0) {
                if (numLines == null) {
                    LOG.trace("Line " + csvRecordLineNumber + " of " + filePath);
                } else {
                    LOG.trace("Line " + csvRecordLineNumber + " / " + numLines + " of " + filePath);
                }
            }

            //if we're restricting the record numbers to process, then check if the new line we're on is one we want to process
            if (recordNumbersToProcess == null
                || recordNumbersToProcess.contains(Long.valueOf(csvRecordLineNumber))) {

                return true;

            } else {
                continue;
            }
        }

        //automatically close the parser once we reach the end, to cut down on memory use
        close();

        return false;
    }

    public Integer getNumLines() {
        return numLines;
    }

    /**
     * moves to the next row in the CSV file, returning false if there is no new row
     *
     */
    private boolean advanceToNextRow() throws Exception {

        try {
            this.csvRecord = this.csvIterator.next();
            this.csvRecordLineNumber = (int)this.csvReader.getCurrentLineNumber(); //safe cast as no CSV file is 2B rows
            return true;

        } catch (NoSuchElementException nse) {
            //a NoSuchElementException is thrown if next() is called when there is no next
            return false;

        } catch (Throwable t) {
            //if we get a throwable that wraps up an IO exception, then it's
            //probably an S3 timeout, in which case we should re-open the file and read forward to where we left off

            //if it's not an IO Exception, just throw it
            if (t.getCause() == null
                    || !(t.getCause() instanceof IOException)) {
                throw t;
            }

            IOException ioe = (IOException)t.getCause();
            LOG.error("Had an IO Exception reading " + filePath);
            LOG.error("" + ioe.getClass().getName() + ": " + ioe.getMessage());

            return reopenAndResumeOnNextLine();
        }
    }

    /**
     * if we had an S3 timeout, this function is called to resume where we left off
     * returns false if there was no next line (i.e. we failed on the last line)
     */
    private boolean reopenAndResumeOnNextLine() throws Exception {

        int nextDesiredRecordNumber = this.csvRecordLineNumber + 1;
        LOG.info("Going to re-open and try to resume on line " + nextDesiredRecordNumber);

        //close everything down
        close();

        //open the file again
        open("Resuming");

        //now read through the rows until we reach the next line number after the last one
        while (true) {
            try {
                this.csvRecord = this.csvIterator.next();
            } catch (NoSuchElementException nse) {
                //if we get this exception, we've actually reached the end of the file (i.e. we failed on the last line)
                return false;
            }

            this.csvRecordLineNumber = (int)this.csvReader.getCurrentLineNumber();

            if (csvRecordLineNumber == nextDesiredRecordNumber) {
                LOG.info("Resuming on line " + csvRecordLineNumber);
                break;
            }
        }

        return true;
    }


    public String getFilePath() {
        return filePath;
    }

    public int getCurrentLineNumber() {
        return csvRecordLineNumber;
    }

    public CsvCurrentState getCurrentState() {
        return new CsvCurrentState(filePath, csvRecordLineNumber);
    }

    /**
     * called to restrict this parser to only processing specific rows
     */
    public void setRecordNumbersToProcess(Set<Long> recordNumbersToProcess) {
        this.recordNumbersToProcess = recordNumbersToProcess;
    }


    public CsvCell getCell(String column) {
        String value = csvRecord.get(column);

        //to save messy handling of non-empty but "empty" strings, trim whitespace of any non-null value
        if (value != null) {
            value = value.trim();
        }

        long rowAuditId = -1;
        if (isFileAudited()) {
            rowAuditId = cellAuditIds[csvRecordLineNumber];
        }

        Integer colIndexObj = csvReader.getHeaderMap().get(column);
        int colIndex = colIndexObj.intValue();

        return new CsvCell(rowAuditId, colIndex, value, dateFormat, timeFormat);
    }





    //remove all the below fns

    public String getString(String column) {
        return csvRecord.get(column);
    }
    public Integer getInt(String column) {
        String s = csvRecord.get(column);
        if (Strings.isNullOrEmpty(s)) {
            return null;
        }

        return Integer.valueOf(s);
    }
    public Long getLong(String column) {
        String s = csvRecord.get(column);
        if (Strings.isNullOrEmpty(s)) {
            return null;
        }

        return Long.valueOf(s);
    }
    public Double getDouble(String column) {
        String s = csvRecord.get(column);
        if (Strings.isNullOrEmpty(s)) {
            return null;
        }

        return new Double(s);
    }
    public Date getDate(String column) throws TransformException {
        String s = csvRecord.get(column);
        if (Strings.isNullOrEmpty(s)) {
            return null;
        }

        try {
            return dateFormat.parse(s);
        } catch (ParseException pe) {
            throw new FileFormatException(filePath, "Invalid date format [" + s + "]", pe);
        }
    }
    public Date getTime(String column) throws TransformException {
        String s = csvRecord.get(column);
        if (Strings.isNullOrEmpty(s)) {
            return null;
        }

        try {
            return timeFormat.parse(s);
        } catch (ParseException pe) {
            throw new FileFormatException(filePath, "Invalid time format [" + s + "]", pe);
        }
    }
    public Date getDateTime(String dateColumn, String timeColumn) throws TransformException {
        Date d = getDate(dateColumn);
        Date t = getTime(timeColumn);
        if (d == null) {
            return null;
        } else if (t == null) {
            return d;
        } else {
            return new Date(d.getTime() + t.getTime());
        }
    }
    public boolean getBoolean(String column) {
        String s = csvRecord.get(column);
        if (Strings.isNullOrEmpty(s)) {
            return false;
        }

        return Boolean.parseBoolean(s);
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
