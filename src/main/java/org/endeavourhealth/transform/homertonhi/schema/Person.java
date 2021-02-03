package org.endeavourhealth.transform.homertonhi.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.homertonhi.HomertonHiCsvToFhirTransformer;

import java.util.UUID;

public class Person extends AbstractCsvParser {

    public Person(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
        HomertonHiCsvToFhirTransformer.CSV_FORMAT,
        HomertonHiCsvToFhirTransformer.DATE_FORMAT,
        HomertonHiCsvToFhirTransformer.TIME_FORMAT);
    }

    //@Override
    protected String[] getCsvHeaders(String version) {

            return new String[] {
                    "empi_id",
                    "birth_date",
                    "birth_date_id",
                    "birth_date_source_type",
                    "birth_date_source_id",
                    "birth_date_source_version",
                    "birth_date_source_description",
                    "birth_date_reference_id",
                    "gender_code",
                    "gender_display",
                    "gender_coding_system_id",
                    "gender_raw_coding_system_id",
                    "gender_raw_code",
                    "gender_source_type",
                    "gender_source_id",
                    "gender_source_version",
                    "gender_source_description",
                    "gender_reference_id",
                    "deceased",
                    "deceased_dt_tm",
                    "deceased_date_id",
                    "deceased_source_type",
                    "deceased_source_id",
                    "deceased_source_version",
                    "deceased_source_description",
                    "deceased_reference_id",
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
                    "address_source_type",
                    "address_source_id",
                    "address_source_version",
                    "address_source_description",
                    "address_reference_id",
                    "phone_type_code",
                    "phone_type_display",
                    "phone_type_coding_system_id",
                    "phone_type_raw_coding_system_id",
                    "phone_type_raw_code",
                    "phone_number",
                    "extension",
                    "country_cd",
                    "phone_source_type",
                    "phone_source_id",
                    "phone_source_version",
                    "phone_source_description",
                    "phone_reference_id",
                    "person_name_type_code",
                    "person_name_type_display",
                    "person_name_coding_system_id",
                    "person_name_raw_coding_system_id",
                    "person_name_raw_code",
                    "full_name",
                    "prefix",
                    "suffix",
                    "given_name1",
                    "given_name2",
                    "given_name3",
                    "family_name1",
                    "family_name2",
                    "family_name3",
                    "title1",
                    "title2",
                    "title3",
                    "name_source_type",
                    "name_source_id",
                    "name_source_version",
                    "name_source_description",
                    "name_reference_id",
                    "population_id",
                    "gender_primary_display",
                    "address_type_primary_display",
                    "state_primary_display",
                    "country_primary_display",
                    "phone_type_primary_display",
                    "person_name_type_primary_display",
                    "record_data_partition_id",
                    "record_person_id",
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

    public CsvCell getBirthDate() { return super.getCell("birth_date"); }

    public CsvCell getGenderCode() { return super.getCell("gender_code"); }

    public CsvCell getIsDecesed() { return super.getCell("deceased"); }

    public CsvCell getDeceasedDtTm() { return super.getCell("deceased_dt_tm"); }

    public CsvCell getAddressTypeCernerCodeSystemId() { return super.getCell("address_type_raw_coding_system_id"); }

    public CsvCell getAddressTypeCernerCode() { return super.getCell("address_type_raw_code"); }

    public CsvCell getAddressLine1() { return super.getCell("address_line_1"); }

    public CsvCell getAddressLine2() { return super.getCell("address_line_2"); }

    public CsvCell getAddressLine3() { return super.getCell("address_line_3"); }

    public CsvCell getAddressCity() { return super.getCell("city"); }

    public CsvCell getAddressPostCode() { return super.getCell("postal_cd"); }

    public CsvCell getAddressCounty() { return super.getCell("county_display"); }

    public CsvCell getPhoneTypeCode() { return super.getCell("phone_type_code"); }

    public CsvCell getPhoneNumber() { return super.getCell("phone_number"); }

    public CsvCell getPhoneExt() { return super.getCell("extension"); }

    public CsvCell getPersonNameTypeCernerCodeSystemId() { return super.getCell("person_name_raw_coding_system_id"); }

    public CsvCell getPersonNameTypeCernerCode() { return super.getCell("person_name_raw_code"); }

    public CsvCell getPersonFullName() { return super.getCell("full_name"); }

    public CsvCell getPersonNamePrefix() { return super.getCell("prefix"); }

    public CsvCell getPersonNameSuffix() { return super.getCell("suffix"); }

    public CsvCell getPersonNameGiven1() { return super.getCell("given_name1"); }

    public CsvCell getPersonNameGiven2() { return super.getCell("given_name2"); }

    public CsvCell getPersonNameFamily() { return super.getCell("family_name1"); }

    public CsvCell getPersonNameTitle() { return super.getCell("title"); }

    public CsvCell getHashValue() { return super.getCell("hash_value"); }
}