package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class PPADD extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(PPADD.class);

    public PPADD(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                null, null); //all Barts date parsing for Power Insight content should use BartsCsvHelper.parseDate(..)
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
                "PRIMARY_CARE_CD",
        };
    }

    public CsvCell getMillenniumAddressId() {
        return super.getCell("#ADDRESS_ID");
    }

    public CsvCell getExtractDateTime() {
        return super.getCell("EXTRACT_DT_TM");
    }

    public CsvCell getActiveIndicator() {
        return super.getCell("ACTIVE_IND");
    }

    public CsvCell getPersonId() {
        return super.getCell("PERSON_ID");
    }

    public CsvCell getBeginEffectiveDate() {
        return super.getCell("BEG_EFFECTIVE_DT_TM");
    }

    public CsvCell getEndEffectiveDate() {
        return super.getCell("END_EFFECTIVE_DT_TM");
    }

    public CsvCell getAddressLine1() {
        return super.getCell("ADDR_LINE1_TXT");
    }

    public CsvCell getAddressLine2() {
        return super.getCell("ADDR_LINE2_TXT");
    }

    public CsvCell getAddressLine3() {
        return super.getCell("ADDR_LINE3_TXT");
    }

    public CsvCell getAddressLine4() {
        return super.getCell("ADDR_LINE4_TXT");
    }

    public CsvCell getCity() {
        return super.getCell("CITY_TXT");
    }

    public CsvCell getPostcode() throws TransformException {
        return super.getCell("POSTCODE_TXT");
    }

    public CsvCell getCountyCode() throws TransformException {
        return super.getCell("COUNTY_CD");
    }

    public CsvCell getCountyText() {
        return super.getCell("COUNTY_TXT");
    }

    public CsvCell getCountryCode() {
        return super.getCell("COUNTRY_CD");
    }

    public CsvCell getCountryText() {
        return super.getCell("COUNTRY_TXT");
    }

    public CsvCell getAddressTypeCode() throws TransformException {
        return super.getCell("ADDRESS_TYPE_CD");
    }

    public CsvCell getAddressTypeSequence() throws TransformException {
        return super.getCell("ADDRESS_TYPE_SEQ_NBR");
    }

    public CsvCell getResidencePCTCodeValue() {
        return super.getCell("PRIMARY_CARE_CD");
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}