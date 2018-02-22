package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.UUID;

public class PPATI extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(PPATI.class);

    public PPATI(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                BartsCsvToFhirTransformer.DATE_FORMAT,
                BartsCsvToFhirTransformer.TIME_FORMAT);
    }

    protected String[] getCsvHeaders(String version) {

        return new String[]{
                "#PERSON_ID",
                "EXTRACT_DT_TM",
                "ACTIVE_IND",
                "LOCAL_PATIENT_IDENT",
                "LOCAL_PATIENT_NHS_ORG_ID",
                "NHS_NBR_IDENT",
                "NHS_NBR_STATUS_CD",
                "BIRTH_DT_TM",
                "EST_BIRTH_DT_CD",
                "GENDER_CD",
                "MARITAL_STATUS_CD",
                "ETHNIC_GROUP_CD",
                "LANGUAGE_CD",
                "RELIGION_CD",
                "DECEASED_DT_TM",
                "CAUSE_OF_DEATH_CD",
                "DECEASED_CD",
                "CONSENT_REL_RELIG_CD"
        };
    }

    public String getMillenniumPersonId() {
        return super.getString("#PERSON_ID");
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

    public String getLocalPatientId() {
        return super.getString("LOCAL_PATIENT_IDENT");
    }

    public String getOrganisationId() {
        return super.getString("LOCAL_PATIENT_NHS_ORG_ID");
    }

    public String getNhsNumber() {
        return super.getString("NHS_NBR_IDENT");
    }

    public String getNhsNumberStatus() {
        return super.getString("NHS_NBR_STATUS_CD");
    }

    public Date getDateOfBirth() throws TransformException {
        return super.getDate("BIRTH_DT_TM");
    }

    public String getDateOfBirthAsString() throws TransformException {
        return super.getString("BIRTH_DT_TM");
    }

    public String getEstimatedBirthDateIndicator() {
        return super.getString("EST_BIRTH_DT_CD");
    }

    public String getGenderCode() {
        return super.getString("GENDER_CD");
    }

    public String getMaritalStatusCode() {
        return super.getString("MARITAL_STATUS_CD");
    }

    public String getEthnicGroupCode() {
        return super.getString("ETHNIC_GROUP_CD");
    }

    public String getFirstLanguageCode() {
        return super.getString("LANGUAGE_CD");
    }

    public String getReligionCode() {
        return super.getString("RELIGION_CD");
    }

    public Date getDeceasedDateTime() throws TransformException {
        if (super.getString("DECEASED_DT_TM").equals("0000-00-00 00:00:00"))
            return null;
        return super.getDate("DECEASED_DT_TM");
    }

    public String getDeceasedDateTimeAsString() throws TransformException {
        return super.getString("DECEASED_DT_TM");
    }

    public String getCauseOfDeathCode() {
        return super.getString("CAUSE_OF_DEATH_CD");
    }

    public String getDeceasedMethodCode() {
        return super.getString("DECEASED_CD");
    }

    public String getConsentToReleaseReligionCode() {
        return super.getString("CONSENT_REL_RELIG_CD");
    }

    @Override
    protected String getFileTypeDescription() {
        return "Cerner person file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
