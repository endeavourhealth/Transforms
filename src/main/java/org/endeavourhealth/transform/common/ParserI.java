package org.endeavourhealth.transform.common;

import java.util.UUID;

public interface ParserI {

    boolean nextRecord() throws Exception;
    CsvCurrentState getCurrentState();
    String getFilePath();
    UUID getServiceId();
    UUID getSystemId();
    UUID getExchangeId();
}
