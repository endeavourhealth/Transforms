package org.endeavourhealth.transform.common;

import com.google.common.base.Strings;
import com.google.common.primitives.Shorts;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.core.csv.CsvHelper;
import org.endeavourhealth.core.database.dal.audit.models.PublishedFileColumn;
import org.endeavourhealth.core.database.dal.audit.models.PublishedFileType;
import org.endeavourhealth.core.exceptions.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

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
    private final DateFormat dateTimeFormat;
    //private final SourceFileMappingDalI dal = DalProvider.factorySourceFileMappingDal();

    private CSVParser csvReader = null;
    private Iterator<CSVRecord> csvIterator = null;
    private CSVRecord csvRecord = null;
    private int csvRecordLineNumber = -1;
    //private Set<Long> recordNumbersToProcess = null;
    private final static String REMOVED_DATA_HEADER = "RemovedData";
    private Charset encoding = null;

    //audit data
    private Integer fileAuditId = null;
    //private long[] cellAuditIds = new long[10000]; //default to 10k audits
    private Integer numLines = null; //only set if we audit the file
    private Map<String, Integer> cachedHeaderMap = null;
    //private CsvAuditorCallbackI auditorCallback = null; //allows selective auditing of records


    public AbstractCsvParser(UUID serviceId, UUID systemId, UUID exchangeId,
                             String version, String filePath, CSVFormat csvFormat,
                             String dateFormat, String timeFormat) {

        this(serviceId, systemId, exchangeId, version, filePath, csvFormat, dateFormat, timeFormat, null);
    }

    public AbstractCsvParser(UUID serviceId, UUID systemId, UUID exchangeId,
                             String version, String filePath, CSVFormat csvFormat,
                             String dateFormat, String timeFormat, Charset encoding) {

        this.serviceId = serviceId;
        this.systemId = systemId;
        this.exchangeId = exchangeId;
        this.version = version;
        this.filePath = filePath;
        this.csvFormat = csvFormat;
        this.encoding = encoding;

        if (!Strings.isNullOrEmpty(dateFormat)) {
            this.dateFormat = new SimpleDateFormat(dateFormat);
        } else {
            this.dateFormat = null;
        }

        if (!Strings.isNullOrEmpty(timeFormat)) {
            this.timeFormat = new SimpleDateFormat(timeFormat);
        } else {
            this.timeFormat = null;
        }

        if (!Strings.isNullOrEmpty(dateFormat) && !Strings.isNullOrEmpty(timeFormat)) {
            this.dateTimeFormat = new SimpleDateFormat(dateFormat + " " + timeFormat);
        } else {
            this.dateTimeFormat = null;
        }

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

    public String getVersion() {
        return version;
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
        InputStreamReader isr = null;
        if (encoding != null) {
            isr = FileHelper.readFileReaderFromSharedStorage(filePath, encoding);
        }
        else {
            isr = FileHelper.readFileReaderFromSharedStorage(filePath);
        }
        this.csvReader = new CSVParser(isr, csvFormat);
        try {
            this.csvIterator = csvReader.iterator();

            String[] expectedHeaders = getCsvHeaders(version);
            try {
                CsvHelper.validateCsvHeaders(csvReader, filePath, expectedHeaders);
            } catch (Exception e) {
                LOG.info(e.getMessage());
                if (filePath.toUpperCase().contains("TPP")) {
                    LOG.error("Retrying in case it's a TPP file with or without RemovedData ");
                    ArrayList<String> headers = new ArrayList(Arrays.asList(expectedHeaders));
                    if (headers.contains(REMOVED_DATA_HEADER)) {
                        headers.remove(REMOVED_DATA_HEADER);
                    } else {
                        headers.add(REMOVED_DATA_HEADER);
                    }
                    expectedHeaders = new String[headers.size()];
                    expectedHeaders = headers.toArray(expectedHeaders);

                    CsvHelper.validateCsvHeaders(csvReader, filePath, expectedHeaders);
                }
            }

            csvRecordLineNumber = 0;

        } catch (Exception e) {
            //if we get any exception thrown during the constructor, make sure to close the reader
            close();
            throw e;
        }
    }

    @Override
    public List<String> getColumnHeaders() {
        List<String> ret = new ArrayList<>();
        String[] expectedHeaders = getCsvHeaders(version);
        for (String s: expectedHeaders) {
            ret.add(s);
        }
        return ret;
    }

    /**
     * when we open this file, this function is called to ensure the content is fully audited. If not, it will use
     * a thread pool to iterate through the file and ensure every row is audited
     */
    public Integer ensureFileAudited() throws Exception {

        //if this file doesn't need auditing, just return out
        if (!isFileAudited()) {
            return null;
        }

        //to work out the Emis CSV version, we create parsers but don't use them to process any records, so
        //detect this by the null service ID and just return out
        if (this.serviceId == null) {
            return null;
        }

        //if we've already audited this file, then just return out
        if (this.fileAuditId != null) {
            return this.fileAuditId;
        }

        //work out if first row is headers or not
        //if the header array in the CSV format is empty, then it means the CSV parser interprets the first
        //record as headers. If it's non-empty, then it means we've had to tell it, so the first row isn't headers
        String[] header = csvFormat.getHeader();
        if (header == null) {
            throw new Exception("Unexpected null header in CSV file");
        }
        boolean firstRecordContainsHeaders = header.length == 0;

        PublishedFileType publishedFileType = new PublishedFileType();
        publishedFileType.setFileType(getClass().getSimpleName());
        publishedFileType.setVariableColumnDelimiter(csvFormat.getDelimiter());
        publishedFileType.setVariableColumnEscape(csvFormat.getEscapeCharacter());
        publishedFileType.setVariableColumnQuote(csvFormat.getQuoteCharacter());

        for (String col: getColumnHeaders()) {
            PublishedFileColumn publishedFileColumn = new PublishedFileColumn();
            publishedFileColumn.setColumnName(col);
            publishedFileType.getColumns().add(publishedFileColumn);
        }

        this.fileAuditId = PublishedFileAuditHelper.auditPublishedFileRecord(this, firstRecordContainsHeaders, publishedFileType);
        return this.fileAuditId;
    }


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

        String firstChars = FileHelper.readFirstCharactersFromSharedStorage(filePath, 5 * 1024); //assuming we never have headers longer than 5KB

        StringReader stringReader = new StringReader(firstChars);
        CSVParser csvReader = new CSVParser(stringReader, csvFormat); //not assigning to class variable as this reader is just for this validation
        try {

            for (String possibleVersion : possibleVersions) {
                String[] expectedHeaders = getCsvHeaders(possibleVersion);
                try {
                    CsvHelper.validateCsvHeaders(csvReader, filePath, expectedHeaders);

                    //if we call the above and don't get an exception, the possible versio is valid for the headers found
                    ret.add(possibleVersion);
                    //LOG.trace("Valid version " + possibleVersion);

                } catch (IllegalArgumentException ex) {
                    //if we get this exception, the headers don't match the possible version
                    //LOG.trace("Not valid version " + possibleVersion + " failed for " + filePath);
                    //LOG.error("Illegal argument exception " + ex.getMessage());
                }
            }

        } finally {
            stringReader.close();
        }

        if (ret.isEmpty()) {
            LOG.error("Ruled out all possible versions because of file " + filePath);
            LOG.error("Headers in file are " + String.join(", ", csvReader.getHeaderMap().keySet()));
            if (filePath.toUpperCase().contains("TPP")) {
                LOG.error("Retrying in case it's a TPP file with or without RemovedData ");
                ret = reTestForValidVersionsForTpp(possibleVersions);
            }
        }

        return ret;
    }

    private List<String> reTestForValidVersionsForTpp(List<String> possibleVersions) throws Exception {
        // Handle tpp files where only difference is we may or may not have the RemovedData column
        // All TPP transforms should include a null check anyway
        List<String> ret = new ArrayList<>();
        String firstChars = FileHelper.readFirstCharactersFromSharedStorage(filePath, 1000); //assuming we never have headers longer than 1000 bytes

        StringReader stringReader = new StringReader(firstChars);
        CSVParser csvReader = new CSVParser(stringReader, csvFormat); //not assigning to class variable as this reader is just for this validation
        try {
            for (String possibleVersion: possibleVersions) {
                String[] expectedHeaders = getCsvHeaders(possibleVersion);
                try {
                    List<String> expectedHeadersPlus = new ArrayList<String>();
                    String[] expectedArray = new String[expectedHeaders.length+1];
                    if (!Arrays.asList(expectedHeaders).contains(REMOVED_DATA_HEADER)) {
                        // If the only difference is we don't have removedData try again with that
                        expectedHeadersPlus.addAll(Arrays.asList(expectedHeaders));
                        expectedHeadersPlus.add(REMOVED_DATA_HEADER);
                        expectedArray = expectedHeadersPlus.toArray(expectedArray);
                    } else {
                        // Check if it works when we remove RemovedData header
                        for (String s : expectedHeaders) {
                            if (!s.equals(REMOVED_DATA_HEADER)) {
                                expectedHeadersPlus.add(s);
                            }
                        }
                        expectedHeadersPlus.removeAll(Arrays.asList("", null));
                        expectedArray = new String[expectedHeadersPlus.size()];
                        expectedArray = expectedHeadersPlus.toArray(expectedArray);
                    }
                    CsvHelper.validateCsvHeaders(csvReader, filePath, expectedArray);
                    //if we call the above and don't get an exception, the possible version is valid for the headers found
                    ret.add(possibleVersion);
                } catch (IllegalArgumentException ex) {
                    //if we get this exception, the headers don't match the possible version. Treat as WAD.
                    LOG.error(ex.getMessage());
                } catch (Exception e) { // I got an IO error which looked to be a special char issue. Try to catch
                    LOG.error(e.getMessage());
                }
            }
        } finally {
            stringReader.close();
        }
        if (ret.isEmpty()) {
            LOG.error("Ruled out all possible versions because of file " + filePath);
            LOG.error("Headers in file are " + String.join(", ", csvReader.getHeaderMap().keySet()));
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

    //just use class name
    //protected abstract String getFileTypeDescription();

    protected abstract boolean isFileAudited();

    public boolean nextRecord() throws Exception {

        //we now only open the first set of parsers when starting a transform, so
        //need to check to open the subsequent ones
        if (csvReader == null) {

            //create (or find if re-processing) an audit entry for this file
            ensureFileAudited();

            //now open the file for reading
            open("Starting");
        }

        //if the source file couldn't be found, the iterator will be null
        if (csvIterator == null) {
            return false;
        }

        try {
            while (advanceToNextRow()) {

                if (this.csvRecordLineNumber % 50000 == 0) {
                    if (numLines == null) {
                        LOG.trace("Line " + csvRecordLineNumber + " of " + filePath);
                    } else {
                        LOG.trace("Line " + csvRecordLineNumber + " / " + numLines + " of " + filePath);
                    }
                }

                //if we're restricting the record numbers to process, then check if the new line we're on is one we want to process
                return true;
                /*if (recordNumbersToProcess == null
                        || recordNumbersToProcess.contains(Long.valueOf(csvRecordLineNumber))) {

                    return true;

                } else {
                    continue;
                }*/
            }
        } catch (ArrayIndexOutOfBoundsException ex) {
            LOG.trace("ArrayIndexOutOfBoundsException at line " + csvRecordLineNumber);
            throw new TransformException("ArrayIndexOutOfBoundsException at line " + csvRecordLineNumber, ex);
        }

        //automatically close the parser once we reach the end, to cut down on memory use
        close();

        return false;
    }

    public Integer getNumLines() {
        return numLines;
    }

    @Override
    public void setNumLines(Integer numLines) {
        this.numLines = numLines;
    }

    /**
     * moves to the next row in the CSV file, returning false if there is no new row
     */
    private boolean advanceToNextRow() throws Exception {

        try {
            this.csvRecord = this.csvIterator.next();
            this.csvRecordLineNumber = (int) this.csvReader.getCurrentLineNumber(); //safe cast as no CSV file is 2B rows
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

            IOException ioe = (IOException) t.getCause();
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

            this.csvRecordLineNumber = (int) this.csvReader.getCurrentLineNumber();

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
    /*public void setRecordNumbersToProcess(Set<Long> recordNumbersToProcess) {
        this.recordNumbersToProcess = recordNumbersToProcess;
    }*/

    @Override
    public CsvCell getCell(String column) {
        String value = null;
        try {
            value = csvRecord.get(column);
        } catch (IllegalArgumentException ex) {
            //if the column doesn't exist, then we'll get this exception, in which case return null
            return null;
        }

        //to save messy handling of non-empty but "empty" strings, trim whitespace of any non-null value
        if (value != null) {
            value = value.replace("\\u00a0", " ").trim(); // replace nbsp with normal space.

            //strip out any high ascii values, i.e. those outside of range 0-127
            value = value.replaceAll("[^\\x00-\\x7F]", "");
        }

        Integer colIndexObj = getCsvReaderHeaderMap().get(column);
        int colIndex = colIndexObj.intValue();

        //ensure we're not exceeding our supported max column count
        if (colIndex > (int)Short.MAX_VALUE) {
            throw new RuntimeException("Column index greater than " + Short.MAX_VALUE);
        }

        if (fileAuditId == null) {
            return new CsvCell(-1, -1, (short)colIndex, value, this);
        } else {
            return new CsvCell(fileAuditId.intValue(), getCurrentLineNumber(), (short)colIndex, value, this);
        }
    }

    public Map<String, Integer> getCsvReaderHeaderMap() {

        //calling getHeaderMap() on the reader will create and return a copy of the map, which
        //is pretty expensive for the number of times that function is called. So we cache a copy and use that.
        if (cachedHeaderMap == null) {
            cachedHeaderMap = csvReader.getHeaderMap();
        }
        return cachedHeaderMap;
    }

    /*public long getSourceFileRecordIdForCurrentRow() {
        if (fileAuditId != null) {
            return cellAuditIds[csvRecordLineNumber];

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

    public Integer getFileAuditId() {
        return fileAuditId;
    }
}
