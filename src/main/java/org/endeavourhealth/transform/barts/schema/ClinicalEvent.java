package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.AbstractCharacterParser;
import org.endeavourhealth.transform.common.exceptions.FileFormatException;
import org.endeavourhealth.transform.common.exceptions.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class ClinicalEvent extends AbstractCharacterParser {
    private static final Logger LOG = LoggerFactory.getLogger(ClinicalEvent.class);

    public static final String DATE_FORMAT = "dd/mm/yyyy";
    public static final String TIME_FORMAT = "hh:mm:ss";
    public static final String DATE_TIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT;

    public ClinicalEvent(String version, String filePath, boolean openParser) throws Exception {
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

    public String getEventId() throws FileFormatException {
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

    public String getPatientId() throws FileFormatException {
        return super.getString("MillenniumPersonIdentifier");
    }

    public String getEncounterId() throws FileFormatException {
        return super.getString("MillenniumEncounterIdentifier");
    }

    public String getEventCode() throws FileFormatException {
        return super.getString("EventMillenniumCode");
    }

    public String getEventCodeClass() throws FileFormatException {
        return super.getString("ClinicalEventClassMillenniumCode");
    }

    public String getEventResultAsText() throws FileFormatException {
        return super.getString("ClinicalEventResult");
    }

    public String getEventUnitsCode() throws FileFormatException {
        return super.getString("ClinicalEventResultUnitsMillenniumCode");
    }

    public String getEventNormalRangeLow() throws FileFormatException {
        return super.getString("NormalValueLowerLimit");
    }

    public String getEventNormalRangeHigh() throws FileFormatException {
        return super.getString("NormalValueUpperLimit");
    }

    public Date getEffectiveDateTime() throws TransformException {
        return super.getDate("ClinicalEventPerformedDateTime");
    }

    public String getEffectiveDateTimeAsString() throws TransformException {
        return super.getString("ClinicalEventPerformedDateTime");
    }

    public String getClinicianID() throws FileFormatException {
            return super.getString("ClinicalEventPerformedMillenniumPersonnelIdentifier");
    }

    public String getEventTag() throws FileFormatException {   //use for display if EventTitleText is null
        return super.getString("EventTag");
    }

    public String getEventTitleText() throws FileFormatException {
        return super.getString("EventTitleText");
    }
}