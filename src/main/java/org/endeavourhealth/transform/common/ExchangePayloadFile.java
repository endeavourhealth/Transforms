package org.endeavourhealth.transform.common;

public class ExchangePayloadFile {

    private String path = null;
    private Long size = null;
    private String type = null;

    public ExchangePayloadFile() {}

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Long getSize() {
        return size;
    }

    public void setSize(Long size) {
        this.size = size;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
