package org.endeavourhealth.transform.common;

import java.text.DateFormat;

public interface ParserI extends HasServiceSystemAndExchangeIdI {

    boolean nextRecord() throws Exception;
    CsvCurrentState getCurrentState();
    String getFilePath();
    long getSourceFileRecordIdForCurrentRow();
    DateFormat getDateFormat();
    DateFormat getTimeFormat();
    DateFormat getDateTimeFormat();
    void setAuditorCallback(CsvAuditorCallbackI auditorCallback);
}
