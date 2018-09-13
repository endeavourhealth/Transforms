package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class PPALI extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(PPALI.class);

    public PPALI(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                BartsCsvToFhirTransformer.DATE_FORMAT,
                BartsCsvToFhirTransformer.TIME_FORMAT);
    }


    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "#PERSON_ALIAS_ID",
                "EXTRACT_DT_TM",
                "ACTIVE_IND",
                "PERSON_ID",
                "BEG_EFFECTIVE_DT_TM",
                "END_EFFECTIVE_DT_TM",
                "ALIAS_TXT",
                "PERSON_ALIAS_TYPE_CD",
                "ALIAS_POOL_CD",
                "PERSON_ALIAS_STATUS_CD",
                "HEALTH_CARD_PROVINCE",
                "HEALTH_CARD_VERSION",
                "HEALTH_CARD_TYPE",
                "HEALTH_CARD_ISSUE_DT_TM",
                "HEALTH_CARD_EXPIRY_DT_TM"
        };
    }

    public CsvCell getMillenniumPersonAliasId() {
        return super.getCell("#PERSON_ALIAS_ID");
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

    public CsvCell getBeginEffectiveDate() {
        return super.getCell("BEG_EFFECTIVE_DT_TM");
    }

    public CsvCell getEndEffectiveDate() {
        return super.getCell("END_EFFECTIVE_DT_TM");
    }

    public CsvCell getAlias() {
        return super.getCell("ALIAS_TXT");
    }

    public CsvCell getAliasTypeCode() {
        return super.getCell("PERSON_ALIAS_TYPE_CD");
    }

    public CsvCell getAliasPoolCode() {
        return super.getCell("ALIAS_POOL_CD");
    }

    public CsvCell getAliasStatusCode() {
        return super.getCell("PERSON_ALIAS_STATUS_CD");
    }

    public CsvCell getHealthCardProvince() {
        return super.getCell("HEALTH_CARD_PROVINCE");
    }

    public CsvCell getHealthCardVersion() {
        return super.getCell("HEALTH_CARD_VERSION");
    }

    public CsvCell getHealthCardType() {
        return super.getCell("HEALTH_CARD_TYPE");
    }

    public CsvCell getHealthCardIssueDateTime() {
        return super.getCell("HEALTH_CARD_ISSUE_DT_TM");
    }

    public CsvCell getHealthCardExpiryDateTime() {
        return super.getCell("HEALTH_CARD_EXPIRY_DT_TM");
    }



    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
