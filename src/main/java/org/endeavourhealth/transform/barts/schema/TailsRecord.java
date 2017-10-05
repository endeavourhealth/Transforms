package org.endeavourhealth.transform.barts.schema;

public class TailsRecord {
    private String CDSUniqueueId;
    private String EncounterId;

    public void setCDSUniqueueId(String CDSUniqueueId) {
        this.CDSUniqueueId = CDSUniqueueId;
    }

    public String getCDSUniqueueId() {
        return CDSUniqueueId;
    }

    public String getEncounterId() {
        return EncounterId;
    }

    public void setEncounterId(String encounterId) {
        EncounterId = encounterId;
    }
}
