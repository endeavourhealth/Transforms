package org.endeavourhealth.transform.emis.csv.schema;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.core.csv.CsvHelper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.CsvCurrentState;
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

public abstract class AbstractCsvParser implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractCsvParser.class);

    private final String version;
    private final String filePath;
    private final CSVFormat csvFormat;
    private final DateFormat dateFormat;
    private final DateFormat timeFormat;

    private CSVParser csvReader = null;
    private Iterator<CSVRecord> csvIterator = null;
    private CSVRecord csvRecord = null;
    private long csvRecordLineNumber = -1;
    private Set<Long> recordNumbersToProcess = null;
    private Integer fileAuditId = null;

    public AbstractCsvParser(String version, String filePath, boolean openParser, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {

        this.version = version;
        this.filePath = filePath;
        this.csvFormat = csvFormat;
        this.dateFormat = new SimpleDateFormat(dateFormat);
        this.timeFormat = new SimpleDateFormat(timeFormat);

        if (openParser) {
            open();
        }
    }


    private void open() throws Exception {

        InputStreamReader isr = FileHelper.readFileReaderFromSharedStorage(filePath);
        this.csvReader = new CSVParser(isr, csvFormat);
        try {
            this.csvIterator = csvReader.iterator();

            String[] expectedHeaders = getCsvHeaders(version);
            CsvHelper.validateCsvHeaders(csvReader, filePath, expectedHeaders);

//            if (this.fileAuditId == null) {
//                SourceFileMappingDalI dal = DalProvider.factorySourceFileMappingDal();
//                dal.auditFile(serviceId, UUID systemId, UUID exchangeId, filePath, String typeDescription, List<String> columns)
//            }

            csvRecordLineNumber = 0;

        } catch (Exception e) {
            //if we get any exception thrown during the constructor, make sure to close the reader
            close();
            throw e;
        }
    }

    public void close() throws IOException {
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

    public boolean nextRecord() throws Exception {

        //we now only open the first set of parsers when starting a transform, so
        //need to check to open the subsequent ones
        if (csvReader == null) {
            open();
        }

        //if the source file couldn't be found, the iterator will be null
        if (csvIterator == null) {
            return false;
        }

        while (advanceToNextRow()) {

            if (this.csvRecordLineNumber % 50000 == 0) {
                LOG.trace("Line " + csvRecordLineNumber + " of " + filePath);
            }

            //if we're restricting the record numbers to process, then check if the new line we're on is one we want to process
            if (recordNumbersToProcess == null
                || recordNumbersToProcess.contains(Long.valueOf(csvRecordLineNumber))) {
                return true;

            } else {
                continue;
            }
        }

        //only log out we "completed" the file if we read any rows from it
        if (csvRecordLineNumber > 1) {
            LOG.info("Completed " + filePath);
        }

        //automatically close the parser once we reach the end, to cut down on memory use
        close();

        return false;
    }

    /**
     * moves to the next row in the CSV file, returning false if there is no new row
     *
     */
    private boolean advanceToNextRow() throws Exception {

        try {
            this.csvRecord = this.csvIterator.next();
            this.csvRecordLineNumber = this.csvReader.getCurrentLineNumber();
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

        long nextDesiredRecordNumber = this.csvRecordLineNumber + 1;
        LOG.info("Going to re-open and try to resume on line " + nextDesiredRecordNumber);

        //close everything down
        close();

        //open the file again
        open();

        //now read through the rows until we reach the next line number after the last one
        while (true) {
            try {
                this.csvRecord = this.csvIterator.next();
            } catch (NoSuchElementException nse) {
                //if we get this exception, we've actually reached the end of the file (i.e. we failed on the last line)
                return false;
            }

            this.csvRecordLineNumber = this.csvReader.getCurrentLineNumber();

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

    public long getCurrentLineNumber() {
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

}
