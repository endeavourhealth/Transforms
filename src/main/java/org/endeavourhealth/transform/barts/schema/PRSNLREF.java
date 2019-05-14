package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class PRSNLREF extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(PRSNLREF.class);

    public PRSNLREF(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                null, null); //all Barts date parsing for Power Insight content should use BartsCsvHelper.parseDate(..)
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "#PERSONNEL_ID",
                "EXTRACT_DT_TM",
                "ACTIVE_IND",
                "POSITION_CD",
                "PHYSICIAN_IND",
                "TITLE_TXT",
                "NAME_FIRST_TXT",
                "NAME_MIDDLE_TXT",
                "NAME_LAST_TXT",
                "NAME_FULL_TXT",
                "BUS_ADDR_LINE1_TXT",
                "BUS_ADDR_LINE2_TXT",
                "BUS_ADDR_LINE3_TXT",
                "BUS_ADDR_LINE4_TXT",
                "BUS_POSTCODE_TXT",
                "BUS_CITY_TXT",
                "BUS_COUNTRY_CD",
                "BUS_COUNTY_CD",
                "BUS_EMAIL_TXT",
                "BUS_FAX_NBR_TXT",
                "BUS_PHONE_NBR_TXT",
                "HCP_NHS_CD_ALIAS_IDENT",
                "MAIN_SPECIALTY_CD",
                "GMP_NHS_IDENT"
        };
    }

    public CsvCell getPersonnelID() {
        return super.getCell("#PERSONNEL_ID");
    }

    public CsvCell getExtractDateTime() {
        return super.getCell("EXTRACT_DT_TM");
    }

    public CsvCell getActiveIndicator() {
        return super.getCell("ACTIVE_IND");
    }

    public CsvCell getMilleniumPositionCode() {
        return super.getCell("POSITION_CD");
    }

    public CsvCell getPhysicianIndicator() {
        return super.getCell("PHYSICIAN_IND");
    }

    public CsvCell getTitle() {
        return super.getCell("TITLE_TXT");
    }

    public CsvCell getFirstName() {
        return super.getCell("NAME_FIRST_TXT");
    }

    public CsvCell getMiddleName() {
        return super.getCell("NAME_MIDDLE_TXT");
    }

    public CsvCell getLastName() {
        return super.getCell("NAME_LAST_TXT");
    }

    public CsvCell getFullFormatName()  {
        return super.getCell("NAME_FULL_TXT");
    }

    public CsvCell getAddress1() {
        return super.getCell("BUS_ADDR_LINE1_TXT");
    }

    public CsvCell getAddress2() {
        return super.getCell("BUS_ADDR_LINE2_TXT");
    }

    public CsvCell getAddress3() {
        return super.getCell("BUS_ADDR_LINE3_TXT");
    }

    public CsvCell getAddress4() {
        return super.getCell("BUS_ADDR_LINE4_TXT");
    }

    public CsvCell getPostCode() {
        return super.getCell("BUS_POSTCODE_TXT");
    }

    public CsvCell getCity() {
        return super.getCell("BUS_CITY_TXT");
    }

    public CsvCell getMillenniumCountryCode() {
        return super.getCell("BUS_COUNTRY_CD");
    }

    public CsvCell getMillenniumCoutryCode() {
        return super.getCell("BUS_COUNTY_CD");
    }

    public CsvCell getEmail() {
        return super.getCell("BUS_EMAIL_TXT");
    }

    public CsvCell getFax() {
        return super.getCell("BUS_FAX_NBR_TXT");
    }

    public CsvCell getPhone() {
        return super.getCell("BUS_PHONE_NBR_TXT");
    }

    public CsvCell getConsultantNHSCode() {
        return super.getCell("HCP_NHS_CD_ALIAS_IDENT");
    }

    public CsvCell getMillenniumSpecialtyCode() {
        return super.getCell("MAIN_SPECIALTY_CD");
    }

    public CsvCell getGPNHSCode() {
        return super.getCell("GMP_NHS_IDENT");
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}