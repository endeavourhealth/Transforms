package org.endeavourhealth.transform.common;

import com.google.common.base.Strings;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.exceptions.FileFormatException;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.TemporalPrecisionEnum;

import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class CsvCell {

    public static final Charset CHARSET = Charset.forName("UTF-8");
    static final ReentrantLock dateFormatLock = new ReentrantLock();
    //changed to store as a UTF-8 encoded byte array to save 50% memory; the additional
    //CPU needed to decode seems minimal (100M encodes & decodes took 20s in testing)
    private byte[] valueBytes;
    //private String value;

    private int colIndex;
    //private long rowAuditId;
    private int publishedFileId;
    private int recordNumber;
    private ParserI parentParser;
    private Long oldStyleAuditId; //temporary until all audits are converted over to new-style

    public CsvCell(int publishedFileId, int recordNumber, int colIndex, String value, ParserI parentParser) {
        this.publishedFileId = publishedFileId;
        this.recordNumber = recordNumber;
        this.colIndex = colIndex;
        this.parentParser = parentParser;

        if (value != null) {
            this.valueBytes = value.getBytes(CHARSET);
        }
        //this.value = value;
    }

    public static CsvCell factoryDummyWrapper(String value) {
        return new CsvCell(-1, -1, -1, value, null);
    }

    public static CsvCell factoryWithNewValue(CsvCell source, String newValue) {
        return new CsvCell(source.getPublishedFileId(), source.getRecordNumber(), source.getColIndex(), newValue, source.getParentParser());
    }

    public static CsvCell factoryOldStyleAudit(Long oldStyleAuditId, int colIndex, String value, ParserI parentParser) {
        CsvCell ret = new CsvCell(-1, -1, colIndex, value, parentParser);
        ret.oldStyleAuditId = oldStyleAuditId;
        return ret;
    }

    public int getColIndex() {
        return colIndex;
    }

    public int getPublishedFileId() {
        return publishedFileId;
    }

    public int getRecordNumber() {
        return recordNumber;
    }

    public Long getOldStyleAuditId() {
        return oldStyleAuditId;
    }

    public ParserI getParentParser() {
        return parentParser;
    }

    public boolean isEmpty() {
        return valueBytes == null || valueBytes.length == 0;
        //return Strings.isNullOrEmpty(value);
    }

    public boolean equalsValue(CsvCell other) {
        if (isEmpty() != other.isEmpty()) {
            return false;

        } else if (isEmpty() && other.isEmpty()) {
            return true;

        } else {
            return getString().equals(other.getString());
        }
    }

    public String getString() {
        if (valueBytes == null) {
            return null;
        } else {
            return new String(valueBytes, CHARSET);
        }
        //return value;
    }
    public Integer getInt() {
        if (isEmpty()) {
            return null;
        }

        return Integer.valueOf(getString());
    }
    public Long getLong() {
        if (isEmpty()) {
            return null;
        }

        return Long.valueOf(getString());
    }
    public Double getDouble() {
        if (isEmpty()) {
            return null;
        }

        return new Double(getString());
    }
    public Date getDate() throws TransformException {
        if (isEmpty()) {
            return null;
        }

        if (parentParser == null) {
            throw new IllegalArgumentException("Can't get getDate on CsvCell that didn't come from a ParserI");
        }

        DateFormat dateFormat = parentParser.getDateFormat();

        //in Barts data, we have a special helper function for parsing dates because they use different formats
        //and are affected by a BST issue (i.e. data is always supplied as UTC), so we give those parsers a null date format to prevent mistakes
        if (dateFormat == null) {
            throw new IllegalArgumentException("No date format on cell - should there be another way of parsing dates for this transform?");
        }

        try {
            dateFormatLock.lock();
            return dateFormat.parse(getString());
        } catch (ParseException pe) {
            throw new FileFormatException("", "Invalid date format [" + getString() + "]", pe);
        }  finally {
           dateFormatLock.unlock();
    }
    }
    public Date getTime() throws TransformException {
        if (isEmpty()) {
            return null;
        }

        if (parentParser == null) {
            throw new IllegalArgumentException("Can't get getTime on CsvCell that didn't come from a ParserI");
        }

        DateFormat timeFormat = parentParser.getTimeFormat();

        //in Barts data, we have a special helper function for parsing dates because they use different formats
        //and are affected by a BST issue (i.e. data is always supplied as UTC), so we give those parsers a null date format to prevent mistakes
        if (timeFormat == null) {
            throw new IllegalArgumentException("No date format on cell - should there be another way of parsing dates for this transform?");
        }

        try {
            dateFormatLock.lock();
            return timeFormat.parse(getString());
        } catch (ParseException pe) {
            throw new FileFormatException("", "Invalid time format [" + getString() + "]", pe);
        } finally {
            dateFormatLock.unlock();
        }
    }
    public Date getDateTime() throws TransformException {
        if (isEmpty()) {
            return null;
        }

        if (parentParser == null) {
            throw new IllegalArgumentException("Can't get getDateTime on CsvCell that didn't come from a ParserI");
        }

        DateFormat dateTimeFormat = parentParser.getDateTimeFormat();

        //in Barts data, we have a special helper function for parsing dates because they use different formats
        //and are affected by a BST issue (i.e. data is always supplied as UTC), so we give those parsers a null date format to prevent mistakes
        if (dateTimeFormat == null) {
            throw new IllegalArgumentException("No date format on cell - should there be another way of parsing dates for this transform?");
        }

        try {
            dateFormatLock.lock();
            return dateTimeFormat.parse(getString());
        } catch (ParseException pe) {
            throw new FileFormatException("", "Invalid date time format [" + getString() + "]", pe);
        } finally {
            dateFormatLock.unlock();
        }
    }
    public boolean getBoolean() {
        if (isEmpty()) {
            return false;
        }

        return Boolean.parseBoolean(getString());
    }
    public boolean getIntAsBoolean() {
        Integer i = getInt();
        if (i == null) {
            return false;
        }
        return i.intValue() == 1;
    }

    /**
     * for logging purposes. To get the value as a String, just use getString()
     */
    public String toString() {
        return "Value [" + getString() + "] PublishedFileId " + publishedFileId + " Record " + recordNumber + " ColIndex " + colIndex;
    }

    public static Date getDateTimeFromTwoCells(CsvCell dateCell, CsvCell timeCell) throws TransformException {
        Date d = null;
        if (dateCell != null) {
            d = dateCell.getDate();
        }

        Date t = null;
        if (timeCell != null) {
            t = timeCell.getTime();
        }

        if (d == null) {
            return null;

        } else if (t == null) {
            return d;

        } else {
            return new Date(d.getTime() + t.getTime());
        }
    }

    /**
     * adds any cells missing from one list to the other (comparing on value only)
     */
    public static void addAnyMissingByValue(List<CsvCell> addTo, List<CsvCell> addFrom) {

        for (CsvCell addFromCell: addFrom) {

            //check if this cell is already in the list and add if not
            boolean alreadyThere = false;
            for (CsvCell addToCell: addTo) {
                if (addToCell.equalsValue(addFromCell)) {
                    alreadyThere = true;
                    break;
                }
            }

            if (!alreadyThere) {
                addTo.add(addFromCell);
            }
        }
    }

    /**
     * removes any matching cells, matching by value only
     */
    public static void removeAnyByValue(List<CsvCell> removeFrom, List<CsvCell> toRemove) {

        for (CsvCell toRemoveCell: toRemove) {

            //remove this cell from the list if found (iterate backwards, so we can remove)
            for (int i=removeFrom.size()-1; i>=0; i--) {
                CsvCell removeFromCell = removeFrom.get(i);
                if (removeFromCell.equalsValue(toRemoveCell)) {
                    removeFrom.remove(i);
                }
            }
        }
    }

    public static DateTimeType getDateTimeType(Date date, String precision) throws TransformException {

        if (date == null) {
            return null;
        }
        if (!Strings.isNullOrEmpty(precision)) {

            switch (precision) {
                case "U":
                    return null;
                case "Y":
                    return new DateTimeType(date, TemporalPrecisionEnum.YEAR);
                case "YM":
                    return new DateTimeType(date, TemporalPrecisionEnum.MONTH);
                case "YMD":
                    return new DateTimeType(date, TemporalPrecisionEnum.DAY);
                case "YMDT":
                    return new DateTimeType(date, TemporalPrecisionEnum.MINUTE);
                default:
                    throw new IllegalArgumentException("Unknown date precision [" + precision + "]");
            }
        } else {
            return null;
        }
    }
}
