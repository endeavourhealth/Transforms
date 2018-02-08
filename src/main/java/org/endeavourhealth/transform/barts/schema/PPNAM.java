package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.AbstractCharacterParser;
import org.endeavourhealth.transform.common.exceptions.FileFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class PPNAM extends AbstractCharacterParser {

    private static final Logger LOG = LoggerFactory.getLogger(PPATI.class);

    public static final String DATE_FORMAT = "dd/mm/yyyy";
    public static final String TIME_FORMAT = "hh:mm:ss";
    public static final String DATE_TIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT;

    public PPNAM(String version, String filePath, boolean openParser) throws Exception {
        super(version, filePath, "\\|", openParser, DATE_FORMAT, TIME_FORMAT);

        addFieldList("MillenniumPersonNameId");
        addFieldList("ExtractDateTime");
        addFieldList("ActiveIndicator");
        addFieldList("MillenniumPersonIdentifier");
        addFieldList("BeginEffectiveDate");
        addFieldList("EndEffectiveDate");
        addFieldList("NameTypeCode");
        addFieldList("FirstName");
        addFieldList("MiddleName");
        addFieldList("LastName");
        addFieldList("Title");
        addFieldList("Prefix");
        addFieldList("Suffix");
        addFieldList("NameTypeSequence");

    }

    public String getMillenniumPersonNameId() throws FileFormatException {
        return super.getString("MillenniumPersonNameId");
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

    public String getNameTypeCode() throws FileFormatException {
        return super.getString("NameTypeCode");
    }

    public String getFirstName() throws FileFormatException {
        return super.getString("FirstName");
    }

    public String getMiddleName() throws FileFormatException {
        return super.getString("MiddleName");
    }

    public String getLastName() throws FileFormatException {
        return super.getString("LastName");
    }

    public String getTitle() throws FileFormatException {
        return super.getString("Title");
    }

    public String getPrefix() throws TransformException {
        return super.getString("Prefix");
    }

    public String getSuffix() throws TransformException {
        return super.getString("Suffix");
    }

    public String getNameTypeSequence() throws FileFormatException {
        return super.getString("NameTypeSequence");
    }
}