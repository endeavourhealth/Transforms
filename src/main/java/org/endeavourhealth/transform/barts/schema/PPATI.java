package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class PPATI extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(PPATI.class);

    public PPATI(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                null, null); //all Barts date parsing for Power Insight content should use BartsCsvHelper.parseDate(..)
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

    public CsvCell getMillenniumPersonId() {
        return super.getCell("#PERSON_ID");
    }

    public CsvCell getExtractDateTime() {
        return super.getCell("EXTRACT_DT_TM");
    }

    public CsvCell getActiveIndicator() {
        return super.getCell("ACTIVE_IND");
    }

    public CsvCell getLocalPatientId() {
        return super.getCell("LOCAL_PATIENT_IDENT");
    }

    public CsvCell getOrganisationId() {
        return super.getCell("LOCAL_PATIENT_NHS_ORG_ID");
    }

    public CsvCell getNhsNumber() {
        return super.getCell("NHS_NBR_IDENT");
    }

    public CsvCell getNhsNumberStatus() {
        return super.getCell("NHS_NBR_STATUS_CD");
    }

    public CsvCell getDateOfBirth() {
        return super.getCell("BIRTH_DT_TM");
    }

    public CsvCell getEstimatedBirthDateIndicator() {
        return super.getCell("EST_BIRTH_DT_CD");
    }

    public CsvCell getGenderCode() {
        return super.getCell("GENDER_CD");
    }

    public CsvCell getMaritalStatusCode() {
        return super.getCell("MARITAL_STATUS_CD");
    }

    public CsvCell getEthnicGroupCode() {
        return super.getCell("ETHNIC_GROUP_CD");
    }

    public CsvCell getFirstLanguageCode() {
        return super.getCell("LANGUAGE_CD");
    }

    public CsvCell getReligionCode() {
        return super.getCell("RELIGION_CD");
    }

    public CsvCell getDeceasedDateTime() {
        return super.getCell("DECEASED_DT_TM");
    }

    public CsvCell getCauseOfDeathCode() {
        return super.getCell("CAUSE_OF_DEATH_CD");
    }

    public CsvCell getDeceasedMethodCode() {
        return super.getCell("DECEASED_CD");
    }

    public CsvCell getConsentToReleaseReligionCode() {
        return super.getCell("CONSENT_REL_RELIG_CD");
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
