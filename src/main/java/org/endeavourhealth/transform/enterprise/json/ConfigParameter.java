package org.endeavourhealth.transform.enterprise.json;

public class ConfigParameter {
    private String fieldName = null;
    private String fieldLabel = null;

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
}
