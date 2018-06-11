package org.endeavourhealth.transform.barts.schema;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class ORGREF extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(ORGREF.class);

    //public static final String DATE_FORMAT = "dd/mm/yyyy";
    //public static final String TIME_FORMAT = "hh:mm:ss";
    //public static final String DATE_TIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT;

    public ORGREF(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath, CSVFormat format) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                format,
                BartsCsvToFhirTransformer.DATE_FORMAT,
                BartsCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "ORG_ID",
                "EXTRACT_DT_TM",
                "ACTIVE_IND",
                "ORG_NAME_TXT",
                "NHS_ORG_ALIAS",
                "PARENT_NHS_ORG_ALIAS",
                "ADDR_LINE1_TXT",
                "ADDR_LINE2_TXT",
                "ADDR_LINE3_TXT",
                "ADDR_LINE4_TXT",
                "POSTCODE_TXT",
                "CITY_TXT",
                "COUNTY_CD",
                "COUNTRY_CD",
                "PHONE_NBR_TXT",
                "FAX_NBR_TXT",
                "EMAIL_TXT"

        };
    }

    public CsvCell getOrdId() { return super.getCell("ORG_ID"); }
    public CsvCell getExtractDateTime() { return super.getCell("EXTRACT_DT_TM"); }
    public CsvCell getActiveInd() { return super.getCell("ACTIVE_IND"); }
    public CsvCell getOrgNameText() { return super.getCell("ORG_NAME_TXT"); }
    public CsvCell getNhsOrgAlias() { return super.getCell("NHS_ORG_ALIAS"); }
    public CsvCell getParentNhsOrgAlias() { return super.getCell("PARENT_NHS_ORG_ALIAS"); }
    public CsvCell getAddrLine1Txt() { return super.getCell("ADDR_LINE1_TXT"); }
    public CsvCell getAddrLine2Txt() { return super.getCell("ADDR_LINE2_TXT"); }
    public CsvCell getAddrLine3Txt() { return super.getCell("ADDR_LINE3_TXT"); }
    public CsvCell getAddrLine4Txt() { return super.getCell("ADDR_LINE4_TXT"); }
    public CsvCell getPostCodeTxt(){ return super.getCell("POSTCODE_TXT"); }
    public CsvCell getCityTxt() { return super.getCell("CITY_TXT"); }
    public CsvCell getCountyCode() { return super.getCell("COUNTY_CD"); }
    public CsvCell getCountryCode() { return super.getCell("COUNTRY_CD"); }
    public CsvCell getPhoneNumberTxt() { return super.getCell("PHONE_NBR_TXT"); }
    public CsvCell getFaxNbrTxt() { return super.getCell("FAX_NBR_TXT"); }
    public CsvCell getEmailTxt() { return super.getCell("EMAIL_TXT");}


    @Override
    protected String getFileTypeDescription() {
        return "Cerner Organization file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }


}