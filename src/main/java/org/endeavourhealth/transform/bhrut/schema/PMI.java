package org.endeavourhealth.transform.bhrut.schema;

import org.endeavourhealth.transform.bhrut.BhrutCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;

import java.util.UUID;

public class PMI extends AbstractCsvParser {

    public PMI(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BhrutCsvToFhirTransformer.CSV_FORMAT,
                BhrutCsvToFhirTransformer.DATE_FORMAT,
                BhrutCsvToFhirTransformer.TIME_FORMAT);
    }

    private static String[] getHeaders(String version) {
        return new String[]{
                "EXTERNAL_ID",
                "PAS_ID",
                "FORENAME",
                "SURNAME",
                "NHS_NUMBER",
                "GENDER_CODE",
                "BIRTH_DTTM",
                "DEATH_DTTM",
                "CAUSEOFDEATH",
                "CAUSEOFDEATH 1B",
                "CAUSEOFDEATH 1c",
                "CAUSEOFDEATH 2",
                "INFECTION_STATUS",
                "ADDRESS1",
                "ADDRESS2",
                "ADDRESS3",
                "ADDRESS4",
                "ADDRESS5",
                "POSTCODE",
                "SENSITIVE_PDS_FLAG",
                "SENSITIVE_LOCAL_FLAG",
                "HPHONE_NUMBER",
                "HPHONE_NUMBER_CONSENT",
                "MPHONE_NUMBER",
                "MPHONE_NUMBER_CONSENT",
                "ETHNICITY_CODE",
                "FOREIGN_LANGUAGE_CODE",
                "NOK_NAME",
                "NOKREL_NHSCODE",
                "REGISTERED_GP_CODE",
                "REGISTERED_GP_PRACTICE",
                "REGISTERED_GP_PRACTICE_CODE",
                "DataUpdateStatus",
        };
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return getHeaders(version);
    }

    public CsvCell getForename() {
        return super.getCell("FORENAME");
    }

    public CsvCell getSurname() {
        return super.getCell("SURNAME");
    }

    public CsvCell getId() {
        CsvCell id = super.getCell("EXTERNAL_ID");
        String newId = "BHRUT-" + id.getString();
        CsvCell ret = new CsvCell(id.getPublishedFileId(), id.getRecordNumber(), id.getColIndex(), newId, id.getParentParser());
        return ret;
    }

    public CsvCell getPasId() {
        return super.getCell("PAS_ID");
    }

    public CsvCell getNhsNumber() {
        return super.getCell("NHS_NUMBER");
    }

    public CsvCell getGender() {
        return super.getCell("GENDER_CODE");
    }

    public CsvCell getDateOfBirth() {
        return super.getCell("BIRTH_DTTM");
    }

    public CsvCell getDateOfDeath() {
        return super.getCell("DEATH_DTTM");
    }

    public CsvCell getCauseOfDeath() {
        return super.getCell("CAUSEOFDEATH");
    }

    public CsvCell getCauseOfDeath1B() {
        return super.getCell("CAUSEOFDEATH 1B");
    }

    public CsvCell getCauseOfDeath1C() {
        return super.getCell("CAUSEOFDEATH 1c");
    }

    public CsvCell getCauseOfDeath2() {
        return super.getCell("CAUSEOFDEATH 2");
    }

    public CsvCell getInfectionStatus() {
        return super.getCell("INFECTION_STATUS");
    }


    public CsvCell getAddress1() {
        return super.getCell("ADDRESS1");
    }

    public CsvCell getAddress2() {
        return super.getCell("ADDRESS2");
    }

    public CsvCell getAddress3() {
        return super.getCell("ADDRESS3");
    }

    public CsvCell getAddress4() {
        return super.getCell("ADDRESS4");
    }

    public CsvCell getAddress5() {
        return super.getCell("ADDRESS5");
    }

    public CsvCell getPostcode() {
        return super.getCell("POSTCODE");
    }

    public CsvCell getSensitivePdsFlag() {
        return super.getCell("SENSITIVE_PDS_FLAG");
    }

    public CsvCell getSensitiveLocalFlag() {
        return super.getCell("SENSITIVE_LOCAL_FLAG");
    }

    public CsvCell getHomePhoneNumber() {
        return super.getCell("HPHONE_NUMBER");
    }

    public CsvCell getHomePhoneNumberConsent() {
        return super.getCell("HPHONE_NUMBER_CONSENT");
    }

    public CsvCell getMobilePhoneNumber() {
        return super.getCell("MPHONE_NUMBER");
    }

    public CsvCell getMobilePhoneNumberConsent() {
        return super.getCell("MPHONE_NUMBER_CONSENT");
    }

    public CsvCell getEthnicityCode() {
        return super.getCell("ETHNICITY_CODE");
    }

    public CsvCell getForeignLanguageCode() {
        return super.getCell("FOREIGN_LANGUAGE_CODE");
    }

    public CsvCell getNextOfKin() {
        return super.getCell("NOK_NAME");
    }

    public CsvCell getNextOfKinCode() {
        return super.getCell("NOKREL_NHSCODE");
    }

    public CsvCell getRegisteredGpCode() {
        return super.getCell("REGISTERED_GP_CODE");
    }

    public CsvCell getRegisteredGpPracticeName() {
        return super.getCell("REGISTERED_GP_PRACTICE");
    }

    public CsvCell getRegisteredGpPracticeCode() {
        return super.getCell("REGISTERED_GP_PRACTICE_CODE");
    }

    public CsvCell getDataUpdateStatus() {
        return super.getCell("DataUpdateStatus");
    }

}
