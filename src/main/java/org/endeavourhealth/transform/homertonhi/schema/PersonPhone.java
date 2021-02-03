package org.endeavourhealth.transform.homertonhi.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.homertonhi.HomertonHiCsvToFhirTransformer;

import java.util.UUID;

public class PersonPhone extends AbstractCsvParser {

    public PersonPhone(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                    "phone_type_code",
                    "phone_type_display",
                    "phone_type_coding_system_id",
                    "phone_type_raw_coding_system_id",
                    "phone_type_raw_code",
                    "phone_number",
                    "extension",
                    "country_cd",
                    "source_type",
                    "source_id",
                    "source_version",
                    "source_description",
                    "population_id",
                    "phone_type_primary_display",
                    "source_type_key",
                    "hash_value",
            };
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getPersonEmpiId() {
        return super.getCell("empi_id");
    }

    public CsvCell getPhoneSequence() {
        return super.getCell("person_seq");
    }

    public CsvCell getPhoneTypeCode() { return super.getCell("phone_type_code"); }

    public CsvCell getPhoneTypeCernerCodeSystemId() { return super.getCell("phone_type_raw_coding_system_id"); }

    public CsvCell getPhoneTypeCernerCode() { return super.getCell("phone_type_raw_code"); }

    public CsvCell getPhoneNumber() { return super.getCell("phone_number"); }

    public CsvCell getPhoneExt() { return super.getCell("extension"); }

    public CsvCell getHashValue() { return super.getCell("hash_value"); }
}