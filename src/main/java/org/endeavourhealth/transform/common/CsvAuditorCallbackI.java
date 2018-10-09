package org.endeavourhealth.transform.common;

/**
 * interface to allow selective auditing of records in a CSV or fixed-width file
 */
public interface CsvAuditorCallbackI {

    public boolean shouldAuditRecord(ParserI parser) throws Exception;
}
