package org.endeavourhealth.transform.homertonrf.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.homertonrf.HomertonRfCsvToFhirTransformer;

import java.util.UUID;

public class PersonLanguage extends AbstractCsvParser {

    public PersonLanguage(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                    "LANG_CODE",
                    "LANG_DISPLAY",
                    "LANG_CODING_SYSTEM_ID",
                    "LANG_RAW_CODING_SYSTEM_ID",
                    "LANG_RAW_CODE",
                    "LANG_SEQ",
                    "SOURCE_TYPE",
                    "SOURCE_ID",
                    "SOURCE_VERSION",
                    "SOURCE_DESCRIPTION",
                    "POPULATION_ID",
                    "LANG_PRIMARY_DISPLAY",
                    "SOURCE_TYPE_KEY",
                    "HASH_VALUE"
            };
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getPersonEmpiId() {
        return super.getCell("EMPI_ID");
    }

    public CsvCell getLanguageSequence() {
        return super.getCell("LANG_SEQ");
    }

    public CsvCell getLanguageDisplay() { return super.getCell("LANG_DISPLAY"); }

    public CsvCell getLanguageCernerCodeSystemId() { return super.getCell("LANG_RAW_CODING_SYSTEM_ID"); }

    public CsvCell getLanguageCernerCode() { return super.getCell("LANG_RAW_CODE"); }

    public CsvCell getHashValue() { return super.getCell("HASH_VALUE"); }
}
