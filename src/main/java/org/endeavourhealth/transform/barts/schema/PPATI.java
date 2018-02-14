package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.AbstractCharacterParser;
import org.endeavourhealth.transform.common.exceptions.FileFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.UUID;

public class PPATI extends AbstractCharacterParser {

    private static final Logger LOG = LoggerFactory.getLogger(PPATI.class);

    public static final String DATE_FORMAT = "dd/mm/yyyy";
    public static final String TIME_FORMAT = "hh:mm:ss";
    public static final String DATE_TIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT;

    public PPATI(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath, boolean openParser) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, "\\|", openParser, DATE_FORMAT, TIME_FORMAT);

        addFieldList("MillenniumPersonId");
        addFieldList("ExtractDateTime");
        addFieldList("ActiveIndicator");
        addFieldList("LocalPatientIdentifier");
        addFieldList("MillenniumOrganisationIdentifier");
        addFieldList("NhsNumber");
        addFieldList("NhsNumberStatus");
        addFieldList("DateOfBirth");
        addFieldList("EstimatedBirthDateIndicator");
        addFieldList("GenderCode");
        addFieldList("MaritalStatusCode");
        addFieldList("EthnicGroupCode");
        addFieldList("FirstLanguageCode");
        addFieldList("ReligionCode");
        addFieldList("DeceasedDateTime");
        addFieldList("CauseOfDeathCode");
        addFieldList("DeceasedMethodCode");
        addFieldList("ConsentToReleaseReligionCode");

    }

    public String getMillenniumPersonId() throws FileFormatException {
        return super.getString("MillenniumPersonId");
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

    public String getLocalPatientId() throws FileFormatException {
        return super.getString("LocalPatientIdentifier");
    }

    public String getOrganisationId() throws FileFormatException {
        return super.getString("MillenniumOrganisationIdentifier");
    }

    public String getNhsNumber() throws FileFormatException {
        return super.getString("NhsNumber");
    }

    public String getNhsNumberStatus() throws FileFormatException {
        return super.getString("NhsNumberStatus");
    }

    public Date getDateOfBirth() throws TransformException {
        return super.getDateTime("DateOfBirth");
    }

    public String getEstimatedBirthDateIndicator() throws FileFormatException {
        return super.getString("EstimatedBirthDateIndicator");
    }

    public String getGenderCode() throws FileFormatException {
        return super.getString("GenderCode");
    }

    public String getMaritalStatusCode() throws FileFormatException {
        return super.getString("MaritalStatusCode");
    }

    public String getEthnicGroupCode() throws TransformException {
        return super.getString("EthnicGroupCode");
    }

    public String getFirstLanguageCode() throws TransformException {
        return super.getString("FirstLanguageCode");
    }

    public String getReligionCode() throws FileFormatException {
        return super.getString("ReligionCode");
    }

    public Date getDeceasedDateTime() throws TransformException {
        return super.getDateTime("DeceasedDateTime");
    }

    public String getCauseOfDeathCode() throws FileFormatException {
        return super.getString("CauseOfDeathCode");
    }

    public String getDeceasedMethodCode() throws FileFormatException {
        return super.getString("DeceasedMethodCode");
    }

    public String getConsentToReleaseReligionCode() throws FileFormatException {
        return super.getString("ConsentToReleaseReligionCode");
    }

    @Override
    protected String getFileTypeDescription() {
        return "Cerner person file";
    }
}
