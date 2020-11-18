package org.endeavourhealth.transform.common.exceptions;

public class UnmappedValueException extends Exception {

    private String value;

    public UnmappedValueException(String message, String value) {
        super(message);
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
