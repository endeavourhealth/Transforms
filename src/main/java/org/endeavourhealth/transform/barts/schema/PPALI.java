package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
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

    public String getMillenniumPersonAliasId() {
        return super.getString("#PERSON_ALIAS_ID");
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

    public String getAlias() {
        return super.getString("ALIAS_TXT");
    }

    public String getAliasTypeCode() {
        return super.getString("PERSON_ALIAS_TYPE_CD");
    }

    public String getAliasPoolCode() {
        return super.getString("ALIAS_POOL_CD");
    }

    public Date getAliasStatusCode() throws TransformException {
        return super.getDate("PERSON_ALIAS_STATUS_CD");
    }

    public Date getHealthCardProvince() throws TransformException {
        return super.getDate("HEALTH_CARD_PROVINCE");
    }

    public String getHealthCardVersion() {
        return super.getString("HEALTH_CARD_VERSION");
    }

    public String getHealthCardType() {
        return super.getString("HEALTH_CARD_TYPE");
    }

    public Date getHealthCardIssueDateTime() throws TransformException {
        return super.getDate("HEALTH_CARD_ISSUE_DT_TM");
    }

    public Date getHealthCardExpiryDateTime() throws TransformException {
        return super.getDate("HEALTH_CARD_EXPIRY_DT_TM");
    }

    @Override
    protected String getFileTypeDescription() {
        return "Cerner person ID file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
