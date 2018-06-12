package org.endeavourhealth.transform.enterprise.json;

public class LinkDistributorModel {
    private String sourceSkid = null;
    private String targetSalkKeyName = null;
    private String targetSkid = null;

    public LinkDistributorModel() {}

    public LinkDistributorModel(String sourceSkid, String targetSalkKeyName, String targetSkid) {
        this.sourceSkid = sourceSkid;
        this.targetSalkKeyName = targetSalkKeyName;
        this.targetSkid = targetSkid;
    }

    public String getSourceSkid() {
        return sourceSkid;
    }

    public void setSourceSkid(String sourceSkid) {
        this.sourceSkid = sourceSkid;
    }

    public String getTargetSalkKeyName() {
        return targetSalkKeyName;
    }

    public void setTargetSalkKeyName(String targetSalkKeyName) {
        this.targetSalkKeyName = targetSalkKeyName;
    }

    public String getTargetSkid() {
        return targetSkid;
    }

    public void setTargetSkid(String targetSkid) {
        this.targetSkid = targetSkid;
    }
}
