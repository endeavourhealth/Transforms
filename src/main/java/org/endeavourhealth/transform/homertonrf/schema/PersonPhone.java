package org.endeavourhealth.transform.homertonrf.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.homertonrf.HomertonRfCsvToFhirTransformer;

import java.util.UUID;

public class PersonPhone extends AbstractCsvParser {

    public PersonPhone(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
        HomertonRfCsvToFhirTransformer.CSV_FORMAT,
        HomertonRfCsvToFhirTransformer.DATE_FORMAT,
        HomertonRfCsvToFhirTransformer.TIME_FORMAT);
    }

    //@Override
    protected String[] getCsvHeaders(String version) {

            return new String[] {
                    "EMPI_ID",
                    "PERSON_SEQ",
                    "PHONE_TYPE_CODE",
                    "PHONE_TYPE_DISPLAY",
                    "PHONE_TYPE_CODING_SYSTEM_ID",
                    "PHONE_TYPE_RAW_CODING_SYSTEM_ID",
                    "PHONE_TYPE_RAW_CODE",
                    "PHONE_NUMBER",
                    "EXTENSION",
                    "COUNTRY_CD",
                    "SOURCE_TYPE",
                    "SOURCE_ID",
                    "SOURCE_VERSION",
                    "SOURCE_DESCRIPTION",
                    "POPULATION_ID",
                    "PHONE_TYPE_PRIMARY_DISPLAY",
                    "SOURCE_TYPE_KEY",
                    "HASH_VALUE",
            };
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getPersonEmpiId() {
        return super.getCell("EMPI_ID");
    }

    public CsvCell getPhoneSequence() {
        return super.getCell("PERSON_SEQ");
    }

    public CsvCell getPhoneTypeCode() { return super.getCell("PHONE_TYPE_CODE"); }

    public CsvCell getPhoneTypeCernerCodeSystemId() { return super.getCell("PHONE_TYPE_RAW_CODING_SYSTEM_ID"); }

    public CsvCell getPhoneTypeCernerCode() { return super.getCell("PHONE_TYPE_RAW_CODE"); }

    public CsvCell getPhoneNumber() { return super.getCell("PHONE_NUMBER"); }

    public CsvCell getPhoneExt() { return super.getCell("EXTENSION"); }

    public CsvCell getHashValue() { return super.getCell("HASH_VALUE"); }
}
