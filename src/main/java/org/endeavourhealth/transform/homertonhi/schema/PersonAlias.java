package org.endeavourhealth.transform.homertonhi.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.homertonhi.HomertonHiCsvToFhirTransformer;

import java.util.UUID;

public class PersonAlias extends AbstractCsvParser {

    public PersonAlias(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
        HomertonHiCsvToFhirTransformer.CSV_FORMAT,
        HomertonHiCsvToFhirTransformer.DATE_FORMAT,
        HomertonHiCsvToFhirTransformer.TIME_FORMAT);
    }

    //@Override
    protected String[] getCsvHeaders(String version) {

            return new String[] {
                    "EMPI_ID",
                    "PERSON_SEQ",
                    "ALIAS_TYPE_CODE",
                    "ALIAS_TYPE_DISPLAY",
                    "ALIAS_TYPE_CODING_SYSTEM_ID",
                    "ALIAS_TYPE_RAW_CODING_SYSTEM_ID",
                    "ALIAS_TYPE_RAW_CODE",
                    "ALIAS",
                    "ASSIGNING_AUTHORITY",
                    "SOURCE_TYPE",
                    "SOURCE_ID",
                    "SOURCE_VERSION",
                    "SOURCE_DESCRIPTION",
                    "POPULATION_ID",
                    "ALIAS_TYPE_PRIMARY_DISPLAY",
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

    public CsvCell getAliasSequence() {
        return super.getCell("PERSON_SEQ");
    }

    public CsvCell getAlias() { return super.getCell("ALIAS"); }

    public CsvCell getAliasTypeCernerCodeSystemId() { return super.getCell("ALIAS_TYPE_RAW_CODING_SYSTEM_ID"); }

    public CsvCell getAliasTypeCernerCode() { return super.getCell("ALIAS_TYPE_RAW_CODE"); }

    public CsvCell getHashValue() { return super.getCell("HASH_VALUE"); }
}
