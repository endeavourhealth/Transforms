package org.endeavourhealth.transform.emis.csv.exceptions;

import org.endeavourhealth.core.database.dal.publisherCommon.models.EmisCodeType;

public class EmisCodeNotFoundException extends Exception {

    private long code;
    private EmisCodeType codeType;

    public EmisCodeNotFoundException(long code, EmisCodeType codeType, String message) {
        super(message);
        this.code = code;
        this.codeType = codeType;
    }

    public long getCode() {
        return code;
    }

    public EmisCodeType getCodeType() {
        return codeType;
    }
}