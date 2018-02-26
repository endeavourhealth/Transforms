package org.endeavourhealth.transform.common;

public interface ParserI {

    boolean nextRecord() throws Exception;
    CsvCurrentState getCurrentState();
}
