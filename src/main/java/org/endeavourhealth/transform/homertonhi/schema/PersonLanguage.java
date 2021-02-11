package org.endeavourhealth.transform.homertonhi.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.homertonhi.HomertonHiCsvToFhirTransformer;

import java.util.UUID;

public class PersonLanguage extends AbstractCsvParser {

    public PersonLanguage(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                    "lang_code",
                    "lang_display",
                    "lang_coding_system_id",
                    "lang_raw_coding_system_id",
                    "lang_raw_code",
                    "lang_seq",
                    "source_type",
                    "source_id",
                    "source_version",
                    "source_description",
                    "population_id",
                    "lang_primary_display",
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

    public CsvCell getLanguageSequence() {
        return super.getCell("lang_seq");
    }

    public CsvCell getLanguageDisplay() { return super.getCell("lang_display"); }

    public CsvCell getLanguagePrimaryDisplay() { return super.getCell("lang_primary_display"); }

    public CsvCell getLanguageCode() { return super.getCell("lang_code"); }

    public CsvCell getLanguageCernerCodeSystemId() { return super.getCell("lang_raw_coding_system_id"); }

    public CsvCell getLanguageCernerCode() { return super.getCell("lang_raw_code"); }

    public CsvCell getHashValue() { return super.getCell("hash_value"); }
}