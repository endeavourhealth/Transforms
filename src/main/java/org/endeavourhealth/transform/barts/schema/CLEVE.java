package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.AbstractCharacterParser;
import org.endeavourhealth.transform.common.exceptions.FileFormatException;
import org.endeavourhealth.transform.common.exceptions.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class CLEVE extends AbstractCharacterParser {
    private static final Logger LOG = LoggerFactory.getLogger(CLEVE.class);

    public static final String DATE_FORMAT = "dd/mm/yyyy";
    public static final String TIME_FORMAT = "hh:mm:ss";
    public static final String DATE_TIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT;

    public CLEVE(String version, String filePath, boolean openParser) throws Exception {
        super(version, filePath, "\\|", openParser, DATE_FORMAT, TIME_FORMAT);

        addFieldList("MillenniumEventId");
        addFieldList("ExtractDateTime");
        addFieldList("ActiveIndicator");
        addFieldList("MillenniumPersonIdentifier");
        addFieldList("MillenniumEncounterIdentifier");
        addFieldList("MillenniumOrderIdentifier");
        addFieldList("MillenniumParentEventIdentifier");
        addFieldList("EventMillenniumCode");
        addFieldList("AccessionNumber");
        addFieldList("EventStartDateTime");
        addFieldList("EventEndDateTime");
        addFieldList("ClinicalSignificanceDateTime");
        addFieldList("ClinicalEventClassMillenniumCode");
        addFieldList("ClinicalEventResultStatusMillenniumCode");
        addFieldList("ClinicalEventResult");
        addFieldList("ClinicalEventNumericResult");
        addFieldList("ClinicalEventResultUnitsMillenniumCode");
        addFieldList("ClinicalEventDateResult");
        addFieldList("NormalcyMillenniumCode");
        addFieldList("NormalValueLowerLimit");
        addFieldList("NormalValueUpperLimit");
        addFieldList("ClinicalEventPerformedDateTime");
        addFieldList("ClinicalEventPerformedMillenniumPersonnelIdentifier");
        addFieldList("ClinicalEventVerifiedDateTime");
        addFieldList("ClinicalEventVerifiedMillenniumPersonnelIdentifier");
        addFieldList("AuthenticFlag");
        addFieldList("CollationSequence");
        addFieldList("CriticalHigh");
        addFieldList("CriticalLow");
        addFieldList("EntryModeMillenniumCode");
        addFieldList("EventRelationMillenniumCode");
        addFieldList("EventTag");
        addFieldList("EventTitleText");
        addFieldList("RecordStatusMillenniumCode");
        addFieldList("ReferenceNumber");
        addFieldList("ValidFromDateTime");
        addFieldList("ContributorSystemMillenniumCode");

    }

    public String getMillenniumEventId() throws FileFormatException {
        return super.getString("MillenniumEventId");
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



}