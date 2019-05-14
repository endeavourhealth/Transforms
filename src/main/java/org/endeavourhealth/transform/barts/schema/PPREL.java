package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class PPREL extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(PPREL.class);
    
    public PPREL(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                null, null); //all Barts date parsing for Power Insight content should use BartsCsvHelper.parseDate(..)
    }


    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "#PERSON_PERSON_RELTN_ID",
                "EXTRACT_DT_TM",
                "ACTIVE_IND",
                "PERSON_ID",
                "RELATED_PERSON_ID",
                "RELATION_CD",
                "RELATION_TYPE_CD",
                "TITLE_TXT",
                "FIRST_NAME_TXT",
                "MIDDLE_NAME_TXT",
                "LAST_NAME_TXT",
                "HOME_ADDR_LINE1_TXT",
                "HOME_ADDR_LINE2_TXT",
                "HOME_ADDR_LINE3_TXT",
                "HOME_ADDR_LINE4_TXT",
                "HOME_CITY_TXT",
                "HOME_COUNTRY_TXT",
                "HOME_POSTCODE_TXT",
                "HOME_PHONE_NBR_TXT",
                "MOB_PHONE_NBR_TXT",
                "WORK_PHONE_NBR_TXT",
                "EMAIL_TXT",
                "BEG_EFFECTIVE_DT_TM",
                "END_EFFECTIVE_DT_TM",
        };

    }

    public CsvCell getMillenniumPersonRelationId() {
        return super.getCell("#PERSON_PERSON_RELTN_ID");
    }

    public CsvCell getExtractDateTime() {
        return super.getCell("EXTRACT_DT_TM");
    }

    public CsvCell getActiveIndicator() {
        return super.getCell("ACTIVE_IND");
    }

    public CsvCell getMillenniumPersonIdentifier() {
        return super.getCell("PERSON_ID");
    }

    public CsvCell getRelatedPersonMillenniumIdentifier() {
        return super.getCell("RELATED_PERSON_ID");
    }

    public CsvCell  getRelationshipToPatientCode() {
        return super.getCell("RELATION_CD");
    }

    public CsvCell getPersonRelationTypeCode() {
        return super.getCell("RELATION_TYPE_CD");
    }

    public CsvCell getTitle() {
        return super.getCell("TITLE_TXT");
    }

    public CsvCell getFirstName() {
        return super.getCell("FIRST_NAME_TXT");
    }

    public CsvCell getMiddleName() {
        return super.getCell("MIDDLE_NAME_TXT");
    }

    public CsvCell getLastName() {
        return super.getCell("LAST_NAME_TXT");
    }

    public CsvCell getAddressLine1() {
        return super.getCell("HOME_ADDR_LINE1_TXT");
    }

    public CsvCell getAddressLine2() {
        return super.getCell("HOME_ADDR_LINE2_TXT");
    }

    public CsvCell getAddressLine3() {
        return super.getCell("HOME_ADDR_LINE3_TXT");
    }

    public CsvCell getAddressLine4() {
        return super.getCell("HOME_ADDR_LINE4_TXT");
    }

    public CsvCell getCity() {
        return super.getCell("HOME_CITY_TXT");
    }

    public CsvCell getCountry() {
        return super.getCell("HOME_COUNTRY_TXT");
    }

    public CsvCell getPostcode() {
        return super.getCell("HOME_POSTCODE_TXT");
    }

    public CsvCell getHomePhoneNumber() {
        return super.getCell("HOME_PHONE_NBR_TXT");
    }

    public CsvCell getMobilePhoneNumber() {
        return super.getCell("MOB_PHONE_NBR_TXT");
    }

    public CsvCell getWorkPhoneNumber() {
        return super.getCell("WORK_PHONE_NBR_TXT");
    }

    public CsvCell getEmailAddress() {
        return super.getCell("EMAIL_TXT");
    }

    public CsvCell getBeginEffectiveDateTime() {
        return super.getCell("BEG_EFFECTIVE_DT_TM");
    }

    public CsvCell getEndEffectiveDateTime() {
        return super.getCell("END_EFFECTIVE_DT_TM");
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}