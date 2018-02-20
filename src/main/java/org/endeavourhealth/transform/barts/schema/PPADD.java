package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.UUID;

public class PPADD extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(PPADD.class);

    public PPADD(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath, boolean openParser) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, openParser,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                BartsCsvToFhirTransformer.DATE_FORMAT,
                BartsCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "#ADDRESS_ID",
                "EXTRACT_DT_TM",
                "ACTIVE_IND",
                "PERSON_ID",
                "BEG_EFFECTIVE_DT_TM",
                "END_EFFECTIVE_DT_TM",
                "ADDR_LINE1_TXT",
                "ADDR_LINE2_TXT",
                "ADDR_LINE3_TXT",
                "ADDR_LINE4_TXT",
                "CITY_TXT",
                "POSTCODE_TXT",
                "COUNTY_CD",
                "COUNTY_TXT",
                "COUNTRY_CD",
                "COUNTRY_TXT",
                "ADDRESS_TYPE_CD",
                "ADDRESS_TYPE_SEQ_NBR",
                "PRIMARE_CARE_CD",
        };
    }

    public String getMillenniumAddressId() {
        return super.getString("#ADDRESS_ID");
    }

    public Date getExtractDateTime() throws TransformException {
        return super.getDate("EXTRACT_DT_TM");
    }

    public String getActiveIndicator() {
        return super.getString("ACTIVE_IND");
    }

    public boolean isActive() {
        int val = super.getInt("ACTIVE_IND");
        if (val == 1) {
            return true;
        } else {
            return false;
        }
    }

    public String getMillenniumPersonIdentifier() {
        return super.getString("PERSON_ID");
    }

    public Date getBeginEffectiveDate() throws TransformException {
        return super.getDate("BEG_EFFECTIVE_DT_TM");
    }

    public Date getEndEffectiveDater() throws TransformException {
        return super.getDate("END_EFFECTIVE_DT_TM");
    }

    public String getAddressLine1() {
        return super.getString("ADDR_LINE1_TXT");
    }

    public String getAddressLine2() {
        return super.getString("ADDR_LINE2_TXT");
    }

    public String getAddressLine3() {
        return super.getString("ADDR_LINE3_TXT");
    }

    public String getAddressLine4() {
        return super.getString("ADDR_LINE4_TXT");
    }

    public String getCity() {
        return super.getString("CITY_TXT");
    }

    public String getPostcode() throws TransformException {
        return super.getString("POSTCODE_TXT");
    }

    public String getCountyCode() throws TransformException {
        return super.getString("COUNTY_CD");
    }

    public String getCountyText() {
        return super.getString("COUNTY_TXT");
    }

    public String getCountryCode() {
        return super.getString("COUNTRY_CD");
    }

    public String getCountryText() {
        return super.getString("COUNTRY_TXT");
    }

    public String getAddressTypeCode() throws TransformException {
        return super.getString("ADDRESS_TYPE_CD");
    }

    public String getAddressTypeSequence() throws TransformException {
        return super.getString("ADDRESS_TYPE_SEQ_NBR");
    }

    public String getResidencePCTCodeValue() {
        return super.getString("PRIMARE_CARE_CD");
    }

    @Override
    protected String getFileTypeDescription() {
        return "Cerner address file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}