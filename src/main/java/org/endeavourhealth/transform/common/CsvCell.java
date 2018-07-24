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

public class CsvCell {

    public static final Charset CHARSET = Charset.forName("UTF-8");

    //changed to store as a UTF-8 encoded byte array to save 50% memory; the additional
    //CPU needed to decode seems minimal (100M encodes & decodes took 20s in testing)
    private byte[] valueBytes;
    //private String value;

    private int colIndex;
    private long rowAuditId;
    private ParserI parentParser;
    /*private DateFormat dateFormat;
    private DateFormat timeFormat;*/

    public CsvCell(long rowAuditId, int colIndex, String value, ParserI parentParser) {
        this.rowAuditId = rowAuditId;
        this.colIndex = colIndex;
        this.parentParser = parentParser;

        if (value != null) {
            this.valueBytes = value.getBytes(CHARSET);
        }
        //this.value = value;
    }

    public static CsvCell factoryDummyWrapper(String value) {
        return new CsvCell(-1, -1, value, null);
    }

    public int getColIndex() {
        return colIndex;
    }

    public long getRowAuditId() {
        return rowAuditId;
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

        try {
            DateFormat dateFormat = parentParser.getDateFormat();
            return dateFormat.parse(getString());
        } catch (ParseException pe) {
            throw new FileFormatException("", "Invalid date format [" + getString() + "]", pe);
        }
    }
    public Date getTime() throws TransformException {
        if (isEmpty()) {
            return null;
        }

        if (parentParser == null) {
            throw new IllegalArgumentException("Can't get getTime on CsvCell that didn't come from a ParserI");
        }

        try {
            DateFormat timeFormat = parentParser.getTimeFormat();
            return timeFormat.parse(getString());
        } catch (ParseException pe) {
            throw new FileFormatException("", "Invalid time format [" + getString() + "]", pe);
        }
    }
    public Date getDateTime() throws TransformException {
        if (isEmpty()) {
            return null;
        }

        if (parentParser == null) {
            throw new IllegalArgumentException("Can't get getDateTime on CsvCell that didn't come from a ParserI");
        }

        try {
            DateFormat dateTimeFormat = parentParser.getDateTimeFormat();
            return dateTimeFormat.parse(getString());
        } catch (ParseException pe) {
            throw new FileFormatException("", "Invalid date time format [" + getString() + "]", pe);
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
        return "Value [" + getString() + "] RowAuditId " + rowAuditId + " ColIndex " + colIndex;
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
