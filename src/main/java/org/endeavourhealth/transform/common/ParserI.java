package org.endeavourhealth.transform.common;

public interface ParserI extends HasServiceSystemAndExchangeIdI {

    boolean nextRecord() throws Exception;
    CsvCurrentState getCurrentState();
    String getFilePath();
    long getSourceFileRecordIdForCurrentRow();
}
