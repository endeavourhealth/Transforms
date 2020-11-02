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
                    "EMPI_ID",
                    "BIRTH_DATE",
                    "BIRTH_DATE_ID",
                    "BIRTH_DATE_SOURCE_TYPE",
                    "BIRTH_DATE_SOURCE_ID",
                    "BIRTH_DATE_SOURCE_VERSION",
                    "BIRTH_DATE_SOURCE_DESCRIPTION",
                    "BIRTH_DATE_REFERENCE_ID",
                    "GENDER_CODE",
                    "GENDER_DISPLAY",
                    "GENDER_CODING_SYSTEM_ID",
                    "GENDER_RAW_CODING_SYSTEM_ID",
                    "GENDER_RAW_CODE",
                    "GENDER_SOURCE_TYPE",
                    "GENDER_SOURCE_ID",
                    "GENDER_SOURCE_VERSION",
                    "GENDER_SOURCE_DESCRIPTION",
                    "GENDER_REFERENCE_ID",
                    "DECEASED",
                    "DECEASED_DT_TM",
                    "DECEASED_DATE_ID",
                    "DECEASED_SOURCE_TYPE",
                    "DECEASED_SOURCE_ID",
                    "DECEASED_SOURCE_VERSION",
                    "DECEASED_SOURCE_DESCRIPTION",
                    "DECEASED_REFERENCE_ID",
                    "ADDRESS_TYPE_CODE",
                    "ADDRESS_TYPE_DISPLAY",
                    "ADDRESS_TYPE_CODING_SYSTEM_ID",
                    "ADDRESS_TYPE_RAW_CODING_SYSTEM_ID",
                    "ADDRESS_TYPE_RAW_CODE",
                    "ADDRESS_LINE_1",
                    "ADDRESS_LINE_2",
                    "ADDRESS_LINE_3",
                    "CITY",
                    "STATE_CODE",
                    "STATE_DISPLAY",
                    "STATE_CODING_SYSTEM_ID",
                    "STATE_RAW_CODING_SYSTEM_ID",
                    "STATE_RAW_CODE",
                    "POSTAL_CD",
                    "COUNTY_CODE",
                    "COUNTY_DISPLAY",
                    "COUNTY_CODING_SYSTEM_ID",
                    "COUNTY_RAW_CODING_SYSTEM_ID",
                    "COUNTY_RAW_CODE",
                    "COUNTRY_CODE",
                    "COUNTRY_DISPLAY",
                    "COUNTRY_CODING_SYSTEM_ID",
                    "COUNTRY_RAW_CODING_SYSTEM_ID",
                    "COUNTRY_RAW_CODE",
                    "ADDRESS_SOURCE_TYPE",
                    "ADDRESS_SOURCE_ID",
                    "ADDRESS_SOURCE_VERSION",
                    "ADDRESS_SOURCE_DESCRIPTION",
                    "ADDRESS_REFERENCE_ID",
                    "PHONE_TYPE_CODE",
                    "PHONE_TYPE_DISPLAY",
                    "PHONE_TYPE_CODING_SYSTEM_ID",
                    "PHONE_TYPE_RAW_CODING_SYSTEM_ID",
                    "PHONE_TYPE_RAW_CODE",
                    "PHONE_NUMBER",
                    "EXTENSION",
                    "COUNTRY_CD",
                    "PHONE_SOURCE_TYPE",
                    "PHONE_SOURCE_ID",
                    "PHONE_SOURCE_VERSION",
                    "PHONE_SOURCE_DESCRIPTION",
                    "PHONE_REFERENCE_ID",
                    "PERSON_NAME_TYPE_CODE",
                    "PERSON_NAME_TYPE_DISPLAY",
                    "PERSON_NAME_CODING_SYSTEM_ID",
                    "PERSON_NAME_RAW_CODING_SYSTEM_ID",
                    "PERSON_NAME_RAW_CODE",
                    "FULL_NAME",
                    "PREFIX",
                    "SUFFIX",
                    "GIVEN_NAME1",
                    "GIVEN_NAME2",
                    "GIVEN_NAME3",
                    "FAMILY_NAME1",
                    "FAMILY_NAME2",
                    "FAMILY_NAME3",
                    "TITLE1",
                    "TITLE2",
                    "TITLE3",
                    "NAME_SOURCE_TYPE",
                    "NAME_SOURCE_ID",
                    "NAME_SOURCE_VERSION",
                    "NAME_SOURCE_DESCRIPTION",
                    "NAME_REFERENCE_ID",
                    "POPULATION_ID",
                    "GENDER_PRIMARY_DISPLAY",
                    "ADDRESS_TYPE_PRIMARY_DISPLAY",
                    "STATE_PRIMARY_DISPLAY",
                    "COUNTRY_PRIMARY_DISPLAY",
                    "PHONE_TYPE_PRIMARY_DISPLAY",
                    "PERSON_NAME_TYPE_PRIMARY_DISPLAY",
                    "RECORD_DATA_PARTITION_ID",
                    "RECORD_PERSON_ID",
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

    public CsvCell getBirthDate() { return super.getCell("BIRTH_DATE"); }

    public CsvCell getGenderCode() { return super.getCell("GENDER_CODE"); }

    public CsvCell getIsDecesed() { return super.getCell("DECEASED"); }

    public CsvCell getDeceasedDtTm() { return super.getCell("DECEASED_DT_TM"); }

    public CsvCell getAddressTypeCernerCodeSystemId() { return super.getCell("ADDRESS_TYPE_RAW_CODING_SYSTEM_ID"); }

    public CsvCell getAddressTypeCernerCode() { return super.getCell("ADDRESS_TYPE_RAW_CODE"); }

    public CsvCell getAddressLine1() { return super.getCell("ADDRESS_LINE_1"); }

    public CsvCell getAddressLine2() { return super.getCell("ADDRESS_LINE_2"); }

    public CsvCell getAddressLine3() { return super.getCell("ADDRESS_LINE_3"); }

    public CsvCell getAddressCity() { return super.getCell("CITY"); }

    public CsvCell getAddressPostCode() { return super.getCell("POSTAL_CD"); }

    public CsvCell getAddressCounty() { return super.getCell("COUNTY_DISPLAY"); }

    public CsvCell getPhoneTypeCode() { return super.getCell("PHONE_TYPE_CODE"); }

    public CsvCell getPhoneNumber() { return super.getCell("PHONE_NUMBER"); }

    public CsvCell getPhoneExt() { return super.getCell("EXTENSION"); }

    public CsvCell getPersonNameTypeCernerCodeSystemId() { return super.getCell("PERSON_NAME_RAW_CODING_SYSTEM_ID"); }

    public CsvCell getPersonNameTypeCernerCode() { return super.getCell("PERSON_NAME_RAW_CODE"); }

    public CsvCell getPersonFullName() { return super.getCell("FULL_NAME"); }

    public CsvCell getPersonNamePrefix() { return super.getCell("PREFIX"); }

    public CsvCell getPersonNameSuffix() { return super.getCell("SUFFIX"); }

    public CsvCell getPersonNameGiven1() { return super.getCell("GIVEN_NAME1"); }

    public CsvCell getPersonNameGiven2() { return super.getCell("GIVEN_NAME2"); }

    public CsvCell getPersonNameFamily() { return super.getCell("FAMILY_NAME1"); }

    public CsvCell getPersonNameTitle() { return super.getCell("TITLE"); }

    public CsvCell getHashValue() { return super.getCell("HASH_VALUE"); }

}
