package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.AbstractCharacterParser;
import org.endeavourhealth.transform.common.exceptions.FileFormatException;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class PPALI extends AbstractCharacterParser {

    private static final Logger LOG = LoggerFactory.getLogger(PPATI.class);

    public static final String DATE_FORMAT = "dd/mm/yyyy";
    public static final String TIME_FORMAT = "hh:mm:ss";
    public static final String DATE_TIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT;

    public PPALI(String version, String filePath, boolean openParser) throws Exception {
        super(version, filePath, "\\|", openParser, DATE_FORMAT, TIME_FORMAT);

        addFieldList("MillenniumPersonAliasId");
        addFieldList("ExtractDateTime");
        addFieldList("ActiveIndicator");
        addFieldList("MillenniumPersonIdentifier");
        addFieldList("BeginEffectiveDate");
        addFieldList("EndEffectiveDate");
        addFieldList("Alias");
        addFieldList("AliasTypeCode");
        addFieldList("AliasPoolCode");
        addFieldList("AliasStatusCode");
        addFieldList("HealthCardProvince");
        addFieldList("HealthCardVersion");
        addFieldList("HealthCardIssueDateTime");
        addFieldList("HealthCardExpiryDateTime");

    }

    public String getMillenniumPersonAliasId() throws FileFormatException {
        return super.getString("MillenniumPersonAliasId");
    }

    public Date getExtractDateTime() throws TransformException {
        return super.getDateTime("ExtractDateTime");
    }

    public String getActiveIndicator() throws FileFormatException {
        return super.getString("ActiveIndicator");
    }

    public boolean isActive() throws FileFormatException {
        int val = super.getInt("ActiveIndicator");
        if (val == 1) {
            return true;
        } else {
            return false;
        }
    }

    public String getMillenniumPersonIdentifier() throws FileFormatException {
        return super.getString("MillenniumPersonIdentifier");
    }

    public Date getBeginEffectiveDate() throws TransformException {
        return super.getDateTime("BeginEffectiveDate");
    }

    public Date getEndEffectiveDater() throws TransformException {
        return super.getDateTime("EndEffectiveDate");
    }

    public String getAlias() throws FileFormatException {
        return super.getString("Alias");
    }

    public String getAliasTypeCode() throws FileFormatException {
        return super.getString("AliasTypeCode");
    }

    public String getAliasPoolCode() throws FileFormatException {
        return super.getString("AliasPoolCode");
    }

    public Date getAliasStatusCode() throws TransformException {
        return super.getDateTime("AliasStatusCode");
    }

    public Date getHealthCardProvince() throws TransformException {
        return super.getDateTime("HealthCardProvince");
    }

    public String getHealthCardVersion() throws FileFormatException {
        return super.getString("HealthCardVersion");
    }

    public Date getHealthCardIssueDateTime() throws TransformException {
        return super.getDateTime("HealthCardIssueDateTime");
    }

    public Date getHealthCardExpiryDateTime() throws TransformException {
        return super.getDateTime("HealthCardExpiryDateTime");
    }
}
