package org.endeavourhealth.transform.common;

/**
 * storage class to use as a map key instead of a string to save memory
 * Strings are arrays of chars, which are two bytes each. By encoding as a UTF-8 byte array,
 * we can halve that. But byte[] doesn't support the equals(..) function as needed for a map key,
 * hence this wrapper object.
 */
public class StringMemorySaver {
    private byte[] bytes = null;

    public StringMemorySaver(String s) {
        this.bytes = s.getBytes(CsvCell.CHARSET);
    }

    public String toString() {
        return new String(bytes, CsvCell.CHARSET);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof StringMemorySaver)) {
            return false;
        }

        StringMemorySaver other = (StringMemorySaver)o;
        if (this.bytes.length != other.bytes.length) {
            return false;
        }

        for (int i=0; i<this.bytes.length; i++) {
            if (this.bytes[i] != other.bytes[i]) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        int h = 1;
        for (int i=0; i<bytes.length; i++) {
            h = 31 * h + (int)bytes[i]; //copied from ByteBuffer
        }
        return h;
    }
}
