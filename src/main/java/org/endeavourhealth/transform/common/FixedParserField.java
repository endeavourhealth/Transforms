package org.endeavourhealth.transform.common;

public class FixedParserField {
    private String name;
    private int fieldPosition;
    private int fieldlength;

    public FixedParserField(String name, int pos, int length) {
        this.name = name;
        this.fieldPosition = pos;
        this.fieldlength = length;
    }

    public String getName() {
        return name;
    }

    public int getFieldPosition() {
        return fieldPosition;
    }

    public int getFieldlength() {
        return fieldlength;
    }
}
