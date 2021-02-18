package org.endeavourhealth.transform.homertonhi.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.homertonhi.HomertonHiCsvToFhirTransformer;

import java.util.UUID;

public class PersonAddress extends AbstractCsvParser {

    public PersonAddress(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                    "address_type_code",
                    "address_type_display",
                    "address_type_coding_system_id",
                    "address_type_raw_coding_system_id",
                    "address_type_raw_code",
                    "address_line_1",
                    "address_line_2",
                    "address_line_3",
                    "city",
                    "state_code",
                    "state_display",
                    "state_coding_system_id",
                    "state_raw_coding_system_id",
                    "state_raw_code",
                    "postal_cd",
                    "county_code",
                    "county_display",
                    "county_coding_system_id",
                    "county_raw_coding_system_id",
                    "county_raw_code",
                    "country_code",
                    "country_display",
                    "country_coding_system_id",
                    "country_raw_coding_system_id",
                    "country_raw_code",
                    "source_type",
                    "source_id",
                    "source_version",
                    "source_description",
                    "population_id",
                    "address_type_primary_display",
                    "state_primary_display",
                    "county_primary_display",
                    "country_primary_display",
                    "begin_effective_dt_tm",
                    "begin_effective_date_id",
                    "end_effective_dt_tm",
                    "end_effective_date_id",
                    "source_type_key",
                    "person_address_latitude",
                    "person_address_longitude",
                    "person_address_geocode_date",
                    "person_address_geocode_match_level",
                    "person_address_geocode_match_type",
                    "person_address_geocode_dt_tm",
                    "person_address_geocode_date_id",
                    "person_address_geocode_time_id",
                    "hash_value"
            };
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getPersonEmpiId() {
        CsvCell id = super.getCell("empi_id");
        String newId = "empi_id-" + id.getString();
        CsvCell ret = new CsvCell(id.getPublishedFileId(), id.getRecordNumber(), id.getColIndex(), newId, id.getParentParser());
        return ret;
    }

    public CsvCell getAddressTypeCernerCodeSystemId() { return super.getCell("address_type_raw_coding_system_id"); }

    public CsvCell getAddressTypeCernerCode() { return super.getCell("address_type_raw_code"); }

    public CsvCell getAddressTypeDisplay() { return super.getCell("address_type_display"); }

    public CsvCell getAddressLine1() { return super.getCell("address_line_1"); }

    public CsvCell getAddressLine2() { return super.getCell("address_line_2"); }

    public CsvCell getAddressLine3() { return super.getCell("address_line_3"); }

    public CsvCell getAddressCity() { return super.getCell("city"); }

    public CsvCell getAddressCounty() { return super.getCell("county_display"); }

    public CsvCell getAddressPostCode() { return super.getCell("postal_cd"); }

    public CsvCell getAddressBeginDtm() { return super.getCell("begin_effective_dt_tm"); }

    public CsvCell getAddressEndDtm() { return super.getCell("end_effective_dt_tm"); }

    public CsvCell getHashValue() { return super.getCell("hash_value"); }
}