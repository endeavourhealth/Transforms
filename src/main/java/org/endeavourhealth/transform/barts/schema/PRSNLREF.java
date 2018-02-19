package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.UUID;

public class PRSNLREF extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(PRSNLREF.class);

    public PRSNLREF(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath, boolean openParser) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, openParser,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                BartsCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD,
                BartsCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "PERSONNEL_ID",
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

    public String getPersonnelID() {
        return super.getString("PERSONNEL_ID");
    }

    public Date getExtractDateTime() throws TransformException {
        return super.getDate("EXTRACT_DT_TM");
    }

    public int getActiveIndicator() {
        return super.getInt("ACTIVE_IND");
    }

    public boolean isActive() {
        int val = super.getInt("ACTIVE_IND");
        if (val == 1) {
            return true;
        } else {
            return false;
        }
    }

    public Long getMilleniumPositionCode() {
        return super.getLong("POSITION_CD");
    }

    public String getPhysicianIndicator() {
        return super.getString("PHYSICIAN_IND");
    }

    public String getTitle() {
        return super.getString("TITLE_TXT");
    }

    public String getFirstName() {
        return super.getString("NAME_FIRST_TXT");
    }

    public String getMiddleName() {
        return super.getString("NAME_MIDDLE_TXT");
    }

    public String getLastName() {
        return super.getString("NAME_LAST_TXT");
    }

    public String getFullFormatName()  {
        return super.getString("NAME_FULL_TXT");
    }

    public String getAddress1() {
        return super.getString("BUS_ADDR_LINE1_TXT");
    }

    public String getAddress2() {
        return super.getString("BUS_ADDR_LINE2_TXT");
    }

    public String getAddress3() {
        return super.getString("BUS_ADDR_LINE3_TXT");
    }

    public String getAddress4() {
        return super.getString("BUS_ADDR_LINE4_TXT");
    }

    public String getPostCode() {
        return super.getString("BUS_POSTCODE_TXT");
    }

    public String getCity() {
        return super.getString("BUS_CITY_TXT");
    }

    public String getMillenniumCountryCode() {
        return super.getString("BUS_COUNTRY_CD");
    }

    public String getMillenniumCoutryCode() {
        return super.getString("BUS_COUNTY_CD");
    }

    public String getEmail() {
        return super.getString("BUS_EMAIL_TXT");
    }

    public String getFax() {
        return super.getString("BUS_FAX_NBR_TXT");
    }

    public String getPhone() {
        return super.getString("BUS_PHONE_NBR_TXT");
    }

    public String getConsultantNHSCode() {
        return super.getString("HCP_NHS_CD_ALIAS_IDENT");
    }

    public Long getMillenniumSpecialtyCode() {
        return super.getLong("MAIN_SPECIALTY_CD");
    }

    public String getGPNHSCode() {
        return super.getString("GMP_NHS_IDENT");
    }

    @Override
    protected String getFileTypeDescription() {
        return "Cerner personnel file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}