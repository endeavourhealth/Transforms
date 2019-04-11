package org.endeavourhealth.transform.subscriber.json;

public class ConfigParameter {
    private String fieldName = null;
    private String fieldLabel = null;
    private String format = null;
    private Boolean mandatory = null;

    public ConfigParameter() {}

    public ConfigParameter(String fieldName, String fieldLabel) {
        this.fieldName = fieldName;
        this.fieldLabel = fieldLabel;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getFieldLabel() {
        return fieldLabel;
    }

    public void setFieldLabel(String fieldLabel) {
        this.fieldLabel = fieldLabel;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public Boolean getMandatory() {
        return mandatory;
    }

    public void setMandatory(Boolean mandatory) {
        this.mandatory = mandatory;
    }
}
