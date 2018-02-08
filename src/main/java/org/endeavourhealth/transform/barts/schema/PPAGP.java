package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.AbstractCharacterParser;
import org.endeavourhealth.transform.common.exceptions.FileFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class PPAGP extends AbstractCharacterParser {

    private static final Logger LOG = LoggerFactory.getLogger(PPATI.class);

    public static final String DATE_FORMAT = "dd/mm/yyyy";
    public static final String TIME_FORMAT = "hh:mm:ss";
    public static final String DATE_TIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT;

    public PPAGP(String version, String filePath, boolean openParser) throws Exception {
        super(version, filePath, "\\|", openParser, DATE_FORMAT, TIME_FORMAT);

        addFieldList("MillenniumPersonPersonnelRelationId");
        addFieldList("ExtractDateTime");
        addFieldList("ActiveIndicator");
        addFieldList("MillenniumPersonIdentifier");
        addFieldList("BeginEffectiveDate");
        addFieldList("EndEffectiveDate");
        addFieldList("RegisteredGPMillenniumId");
        addFieldList("RegisteredGPPracticeMillenniumIdOrganisationCode");
        addFieldList("PersonPersonnelRelationCode");

    }

    public String getMillenniumPersonPersonnelRelationId() throws FileFormatException {
        return super.getString("MillenniumPersonPersonnelRelationId");
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

    public String getRegisteredGPMillenniumId() throws FileFormatException {
        return super.getString("RegisteredGPMillenniumId");
    }

    public String getRegisteredGPPracticeMillenniumIdOrganisationCode() throws FileFormatException {
        return super.getString("RegisteredGPPracticeMillenniumIdOrganisationCode");
    }

    public String getPersonPersonnelRelationCode() throws FileFormatException {
        return super.getString("PersonPersonnelRelationCode");
    }
}

