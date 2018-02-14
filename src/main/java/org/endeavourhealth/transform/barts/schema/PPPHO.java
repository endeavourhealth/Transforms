package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.AbstractCharacterParser;
import org.endeavourhealth.transform.common.exceptions.FileFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.UUID;

public class PPPHO extends AbstractCharacterParser {

    private static final Logger LOG = LoggerFactory.getLogger(PPATI.class);

    public static final String DATE_FORMAT = "dd/mm/yyyy";
    public static final String TIME_FORMAT = "hh:mm:ss";
    public static final String DATE_TIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT;

    public PPPHO(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath, boolean openParser) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, "\\|", openParser, DATE_FORMAT, TIME_FORMAT);

        addFieldList("MillenniumPhoneId");
        addFieldList("ExtractDateTime");
        addFieldList("ActiveIndicator");
        addFieldList("MillenniumPersonIdentifier");
        addFieldList("BeginEffectiveDateTime");
        addFieldList("EndEffectiveDateTime");
        addFieldList("PhoneTypeCode");
        addFieldList("PhoneTypeSequence");
        addFieldList("PhoneNumber");
        addFieldList("Extension");
        addFieldList("ContactMethodCode");

    }

    public String getMillenniumPhoneId() throws FileFormatException {
        return super.getString("MillenniumPhoneId");
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

    public Date getBeginEffectiveDateTime() throws TransformException {
        return super.getDateTime("BeginEffectiveDateTime");
    }

    public Date getEndEffectiveDateTime() throws TransformException {
        return super.getDateTime("EndEffectiveDateTime");
    }

    public String getPhoneTypeCode() throws FileFormatException {
        return super.getString("PhoneTypeCode");
    }

    public String getPhoneTypeSequence() throws FileFormatException {
        return super.getString("PhoneTypeSequence");
    }

    public String getPhoneNumber() throws FileFormatException {
        return super.getString("PhoneNumber");
    }

    public String getExtension() throws FileFormatException {
        return super.getString("Extension");
    }

    public String getContactMethodCode() throws FileFormatException {
        return super.getString("ContactMethodCode");
    }

    @Override
    protected String getFileTypeDescription() {
        return "Cerner person phone file";
    }
}
