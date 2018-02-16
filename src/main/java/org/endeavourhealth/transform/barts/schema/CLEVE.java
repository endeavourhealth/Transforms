package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.AbstractCharacterParser;
import org.endeavourhealth.transform.common.exceptions.FileFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.UUID;

public class CLEVE extends AbstractCharacterParser {
    private static final Logger LOG = LoggerFactory.getLogger(CLEVE.class);

    public static final String DATE_FORMAT = "dd/mm/yyyy";
    public static final String TIME_FORMAT = "hh:mm:ss";
    public static final String DATE_TIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT;
    public static final int RECORD_ACTIVE = 1;

    public CLEVE(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath, boolean openParser) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, "\\|", openParser, DATE_FORMAT, TIME_FORMAT);

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
        return (super.getInt("ActiveIndicator") == RECORD_ACTIVE);
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

    public String getEventResultClassCode() throws FileFormatException {
        return super.getString("ClinicalEventResultStatusMillenniumCode");
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

    public String getEventNormalcyCode() throws  FileFormatException {
        return super.getString("NormalcyMillenniumCode");
    }
    public String getEventNormalRangeHigh() throws FileFormatException {
        return super.getString("NormalValueUpperLimit");
    }

    public Date getEffectiveDateTime() throws TransformException {
        return super.getDate("ClinicalSignificanceDateTime");
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

    public String getRecordStatusreference() throws FileFormatException {
        return super.getString("RecordStatusMillenniumCode");
    }

    //TODO Do we need some boolean methods on status?
    public String getReferenceNumber() throws FileFormatException {
        return super.getString("ReferenceNumber");
    }

    public Date getValidFromDateTime() throws TransformException {
        return super.getDate("ValidFromDateTime");
    }

    public String getValidFromDateTimeasString() throws TransformException {
        return super.getString("ValidFromDateTime");
    }

    public String getContributorSystemMillenniumCode() throws FileFormatException {
        return super.getString("ContributorSystemMillenniumCode");
    }
    @Override
    protected String getFileTypeDescription() {
        return "Cerner clinical events file";
    }
}