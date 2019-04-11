package org.endeavourhealth.transform.subscriber.json;

import java.util.List;

public class LinkDistributorConfig {
    private String saltKeyName = null;
    private String salt = null;
    private List<ConfigParameter> parameters;

    private LinkDistributorConfig() {}

    private LinkDistributorConfig(String saltKeyName, String salt, List<ConfigParameter> parameters) {
        this.saltKeyName = saltKeyName;
        this.salt = salt;
        this.parameters = parameters;
    }

    public String getSaltKeyName() {
        return saltKeyName;
    }

    public void setSaltKeyName(String saltKeyName) {
        this.saltKeyName = saltKeyName;
    }

    public String getSalt() {
        return salt;
    }

    public void setSalt(String salt) {
        this.salt = salt;
    }

    public List<ConfigParameter> getParameters() {
        return parameters;
    }

    public void setParameters(List<ConfigParameter> parameters) {
        this.parameters = parameters;
    }
}
