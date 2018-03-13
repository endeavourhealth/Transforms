package org.endeavourhealth.transform.common;

import com.google.common.base.Strings;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.exceptions.FileFormatException;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.List;

public class CsvCell {
    private String value;
    private int colIndex;
    private long rowAuditId;
    private ParserI parentParser;
    /*private DateFormat dateFormat;
    private DateFormat timeFormat;*/

    public CsvCell(long rowAuditId, int colIndex, String value, ParserI parentParser) {
        this.rowAuditId = rowAuditId;
        this.colIndex = colIndex;
        this.value = value;
        this.parentParser = parentParser;
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
        return Strings.isNullOrEmpty(value);
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
        return value;
    }
    public Integer getInt() {
        if (Strings.isNullOrEmpty(value)) {
            return null;
        }

        return Integer.valueOf(value);
    }
    public Long getLong() {
        if (Strings.isNullOrEmpty(value)) {
            return null;
        }

        return Long.valueOf(value);
    }
    public Double getDouble() {
        if (Strings.isNullOrEmpty(value)) {
            return null;
        }

        return new Double(value);
    }
    public Date getDate() throws TransformException {
        if (Strings.isNullOrEmpty(value)) {
            return null;
        }

        if (parentParser == null) {
            throw new IllegalArgumentException("Can't get getDate on CsvCell that didn't come from a ParserI");
        }

        try {
            DateFormat dateFormat = parentParser.getDateFormat();
            return dateFormat.parse(value);
        } catch (ParseException pe) {
            throw new FileFormatException("", "Invalid date format [" + value + "]", pe);
        }
    }
    public Date getTime() throws TransformException {
        if (Strings.isNullOrEmpty(value)) {
            return null;
        }

        if (parentParser == null) {
            throw new IllegalArgumentException("Can't get getTime on CsvCell that didn't come from a ParserI");
        }

        try {
            DateFormat timeFormat = parentParser.getTimeFormat();
            return timeFormat.parse(value);
        } catch (ParseException pe) {
            throw new FileFormatException("", "Invalid time format [" + value + "]", pe);
        }
    }
    public boolean getBoolean() {
        if (Strings.isNullOrEmpty(value)) {
            return false;
        }

        return Boolean.parseBoolean(value);
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
        return "Value [" + value + "] RowAuditId " + rowAuditId + " ColIndex " + colIndex;
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
}
