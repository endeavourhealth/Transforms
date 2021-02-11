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
                    "empi_id",
                    "person_seq",
                    "alias_type_code",
                    "alias_type_display",
                    "alias_type_coding_system_id",
                    "alias_type_raw_coding_system_id",
                    "alias_type_raw_code",
                    "alias",
                    "assigning_authority",
                    "source_type",
                    "source_id",
                    "source_version",
                    "source_description",
                    "population_id",
                    "alias_type_primary_display",
                    "source_type_key",
                    "hash_value"
            };
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getPersonEmpiId() {
        return super.getCell("empi_id");
    }

    public CsvCell getAliasSequence() {
        return super.getCell("person_seq");
    }

    public CsvCell getAlias() { return super.getCell("alias"); }

    public CsvCell getAliasTypeCernerCodeSystemId() { return super.getCell("alias_type_raw_coding_system_id"); }

    public CsvCell getAliasTypeCernerCode() { return super.getCell("alias_type_raw_code"); }

    public CsvCell getAliasTypeDisplay() { return super.getCell("alias_type_display"); }

    public CsvCell getHashValue() { return super.getCell("hash_value"); }
}