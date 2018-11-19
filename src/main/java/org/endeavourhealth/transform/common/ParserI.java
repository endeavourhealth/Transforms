package org.endeavourhealth.transform.common;

import java.text.DateFormat;
import java.util.List;

public interface ParserI extends HasServiceSystemAndExchangeIdI {

    boolean nextRecord() throws Exception;
    CsvCurrentState getCurrentState();
    String getFilePath();
    long getSourceFileRecordIdForCurrentRow();
    DateFormat getDateFormat();
    DateFormat getTimeFormat();
    DateFormat getDateTimeFormat();
    void setAuditorCallback(CsvAuditorCallbackI auditorCallback);
    List<String> getColumnHeaders();
    CsvCell getCell(String column);
}
