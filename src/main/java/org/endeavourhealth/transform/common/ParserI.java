package org.endeavourhealth.transform.common;

import java.text.DateFormat;
import java.util.List;

public interface ParserI extends HasServiceSystemAndExchangeIdI {

    boolean nextRecord() throws Exception;
    CsvCurrentState getCurrentState();
    String getFilePath();
    String getVersion();
    //long getSourceFileRecordIdForCurrentRow();
    DateFormat getDateFormat();
    DateFormat getTimeFormat();
    DateFormat getDateTimeFormat();
    //void setAuditorCallback(CsvAuditorCallbackI auditorCallback);
    List<String> getColumnHeaders();
    CsvCell getCell(String column);
    //void setRowAuditId(int recordNumber, Long rowAuditId);
    void setNumLines(Integer numLines);

    Integer ensureFileAudited() throws Exception;
}
