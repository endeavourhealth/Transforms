package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.AbstractCharacterParser;
import org.endeavourhealth.transform.common.exceptions.FileFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.UUID;

public class ENCNT extends AbstractCharacterParser {
    private static final Logger LOG = LoggerFactory.getLogger(ENCNT.class);

    public static final String DATE_FORMAT = "dd/mm/yyyy";
    public static final String TIME_FORMAT = "hh:mm:ss";
    public static final String DATE_TIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT;

    public ENCNT(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath, boolean openParser) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, "\\|", openParser, DATE_FORMAT, TIME_FORMAT);

        addFieldList("MillenniumEncounterIdentifier");
        addFieldList("ExtractDateTime");
        addFieldList("ActiveIndicator");
        addFieldList("MillenniumPersonIdentifier");
        addFieldList("MillenniumFinancialNumberIdentifier");
        addFieldList("EncounterCreateDateTime");
        addFieldList("EncounterTypeMillenniumCode");
        addFieldList("EncounterStatusMillenniumCode");
        addFieldList("MilleniumSourceIdentifierForVisit");
        addFieldList("ReferralWrittenDate");
        addFieldList("ReferalReceivedDate");
        addFieldList("SourceofReferralMillenniumCode");
        addFieldList("ReferrerMillenniumPersonnelIdentifier");
        addFieldList("ReasonForVisitText");
        addFieldList("CurrentMainSpecialtyMillenniumCode");
        addFieldList("CurrentTreatmentFunctionMillenniumCode");
        addFieldList("CurrenrLocalSpecialtyCode");
        addFieldList("CurrentInstitutionMillenniumLocationCode");
        addFieldList("MillenniumAdministrativeCategoty");
        addFieldList("EpisodeIdentifier");
        addFieldList("ResponsibleHealthCareprovidingPersonnelIdentifier");
        addFieldList("RegisteringMillenniumPersonnelIdentifier");
        addFieldList("CurrentLocationIdentifier");

    }

    public String getMillenniumEncounterIdentifier() throws FileFormatException {
        return super.getString("MillenniumEncounterIdentifier");
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
        return super.getString("MillenniumPersonIdentifier");}

    public String getMillenniumFinancialNumberIdentifier() throws FileFormatException {
        return super.getString("MillenniumFinancialNumberIdentifier");}

    public Date getEncounterCreateDateTime() throws TransformException {
        return super.getDateTime("EncounterCreateDateTime");}

    public String getEncounterTypeMillenniumCode() throws FileFormatException {
        return super.getString("EncounterTypeMillenniumCode");}

    public String getEncounterStatusMillenniumCode() throws FileFormatException {
        return super.getString("EncounterStatusMillenniumCode");}

    public String getMilleniumSourceIdentifierForVisit() throws FileFormatException {
        return super.getString("MilleniumSourceIdentifierForVisit");}

    public Date getReferralWrittenDate() throws TransformException {
        return super.getDateTime("ReferralWrittenDate");}

    public Date getReferalReceivedDate() throws TransformException {
        return super.getDateTime("ReferalReceivedDate");}

    public String getSourceofReferralMillenniumCode() throws FileFormatException {
        return super.getString("SourceofReferralMillenniumCode");}

    public String getReferrerMillenniumPersonnelIdentifier() throws FileFormatException {
        return super.getString("ReferrerMillenniumPersonnelIdentifier");}

    public String getReasonForVisitText() throws FileFormatException {
        return super.getString("ReasonForVisitText");}

    public String getCurrentMainSpecialtyMillenniumCode() throws FileFormatException {
        return super.getString("CurrentMainSpecialtyMillenniumCode");}

    public String getCurrentTreatmentFunctionMillenniumCode() throws FileFormatException {
        return super.getString("CurrentTreatmentFunctionMillenniumCode");}

    public String getCurrenrLocalSpecialtyCode() throws FileFormatException {
        return super.getString("CurrenrLocalSpecialtyCode");}

    public String getCurrentInstitutionMillenniumLocationCode() throws FileFormatException {
        return super.getString("CurrentInstitutionMillenniumLocationCode");}

    public String getMillenniumAdministrativeCategoty() throws FileFormatException {
        return super.getString("MillenniumAdministrativeCategoty");}

    public String getEpisodeIdentifier() throws FileFormatException {
        return super.getString("EpisodeIdentifier");}

    public String getResponsibleHealthCareprovidingPersonnelIdentifier() throws FileFormatException {
        return super.getString("ResponsibleHealthCareprovidingPersonnelIdentifier");}

    public String getRegisteringMillenniumPersonnelIdentifier() throws FileFormatException {
        return super.getString("RegisteringMillenniumPersonnelIdentifier");}

    public String getCurrentLocationIdentifier() throws FileFormatException {
        return super.getString("CurrentLocationIdentifier");}

    @Override
    protected String getFileTypeDescription() {
        return "Cerner encounter file";
    }
}