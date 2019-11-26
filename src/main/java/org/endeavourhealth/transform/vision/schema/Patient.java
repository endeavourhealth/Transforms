package org.endeavourhealth.transform.vision.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.vision.VisionCsvToFhirTransformer;

import java.util.UUID;

public class Patient extends AbstractCsvParser {

    public Patient(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                VisionCsvToFhirTransformer.CSV_FORMAT.withHeader(getHeaders(version)),
                VisionCsvToFhirTransformer.DATE_FORMAT,
                VisionCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return getHeaders(version);
    }

    private static String[] getHeaders(String version) {

        if (version.equals(VisionCsvToFhirTransformer.VERSION_TEST_PACK)) {
            //the test pack file is missing a number of columns that the live version has, the phone and email ones
            return new String[]{
                    "PID",
                    "REFERENCE",
                    "DATE_OF_BIRTH",
                    "SEX",
                    "POSTCODE",
                    "MARITAL_STATUS",
                    "GP",
                    "GP_USUAL",
                    "ACTIVE",
                    "REGISTERED_DATE",
                    "REMOVED_DATE",
                    "HA",
                    "PCG",
                    "SURGERY",
                    "MILEAGE",
                    "DISPENSING",
                    "ETHNIC",
                    "DATE_OF_DEATH",
                    "PRACTICE",
                    "SURNAME",
                    "FORENAME",
                    "TITLE",
                    "NHS_NUMBER",
                    "ADDRESS",
                    "ADDRESS_1",
                    "ADDRESS_2",
                    "ADDRESS_3",
                    "ADDRESS_4",
                    "ADDRESS_5",
                    "PRACT_NUMBER",
                    "SERVICE_ID",
                    "ACTION"
            };
        } else {
            return new String[]{
                    "PID",
                    "REFERENCE",
                    "DATE_OF_BIRTH",
                    "SEX",
                    "POSTCODE",
                    "MARITAL_STATUS",
                    "GP",
                    "GP_USUAL",
                    "ACTIVE",
                    "REGISTERED_DATE",
                    "REMOVED_DATE",
                    "HA",
                    "PCG",
                    "SURGERY",
                    "MILEAGE",
                    "DISPENSING",
                    "ETHNIC",
                    "DATE_OF_DEATH",
                    "PRACTICE",
                    "SURNAME",
                    "FORENAME",
                    "TITLE",
                    "NHS_NUMBER",
                    "ADDRESS",
                    "ADDRESS_1",
                    "ADDRESS_2",
                    "ADDRESS_3",
                    "ADDRESS_4",
                    "ADDRESS_5",
                    "PHONE_NUMBER",
                    "MOBILE_NUMBER",
                    "EMAIL",
                    "PRACT_NUMBER",
                    "SERVICE_ID",
                    "ACTION"
            };
        }
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getPatientID() {
        return super.getCell("PID");
    }

    public CsvCell getPatientRef() {
        return super.getCell("REFERENCE");
    }

    public CsvCell getOrganisationID() {
        return super.getCell("SERVICE_ID");
    }

    public CsvCell getSex() {
        return super.getCell("SEX");
    }

    /**
     * note we derive ethnicity from the Journal records with ethnicity codes, so this field is ignored
     */
    public CsvCell getEthnicOrigin() {
        return super.getCell("ETHNIC");
    }

    public CsvCell getMaritalStatus() {
        return super.getCell("MARITAL_STATUS");
    }

    public CsvCell getDateOfBirth() {
        return super.getCell("DATE_OF_BIRTH");
    }

    public CsvCell getDateOfDeath() {
        return super.getCell("DATE_OF_DEATH");
    }

    public CsvCell getTitle() {
        return super.getCell("TITLE");
    }

    public CsvCell getGivenName() {
        return super.getCell("FORENAME");
    }

    public CsvCell getSurname() {
        return super.getCell("SURNAME");
    }

    public CsvCell getDateOfRegistration() {
        return super.getCell("REGISTERED_DATE");
    }

    public CsvCell getNhsNumber() {
        return super.getCell("NHS_NUMBER");
    }

    public CsvCell getPatientNumber() {
        return super.getCell("PRACT_NUMBER");
    }

    /**
     * field called ACTIVE but contains registration type codes
         R	Currently registered for GMS
         T	Temporary
         S	Not registered for GMS but is registered for another service category (e.g. contraception or child health)
         D	Deceased
         L	Left practice (no longer registered)
         P	Private patient
     */
    public CsvCell getPatientTypeCode() {
        return super.getCell("ACTIVE");
    }

    /**
     * same content as other ADDRESS_x fields but combined and pipe-delimited
     */
    public CsvCell getAddressCombined() {
        return super.getCell("ADDRESS");
    }

    public CsvCell getHouseNameFlatNumber() {
        return super.getCell("ADDRESS_1");
    }

    public CsvCell getNumberAndStreet() {
        return super.getCell("ADDRESS_2");
    }

    public CsvCell getVillage() {
        return super.getCell("ADDRESS_3");
    }

    public CsvCell getTown() {
        return super.getCell("ADDRESS_4");
    }

    public CsvCell getCounty() {
        return super.getCell("ADDRESS_5");
    }

    public CsvCell getPostcode() {
        return super.getCell("POSTCODE");
    }

    public CsvCell getDateOfDeactivation() {
        return super.getCell("REMOVED_DATE");
    }

    public CsvCell getHomePhone() {
        return super.getCell("PHONE_NUMBER");
    }

    public CsvCell getMobilePhone() {
        return super.getCell("MOBILE_NUMBER");
    }

    public CsvCell getEmail() {
        return super.getCell("EMAIL");
    }

    public CsvCell getUsualGpId() {
        return super.getCell("GP_USUAL");
    }

    /**
     * from Vision spec - this field is the old-style "registered" GP
     */
    public CsvCell getRegisteredGpId() {
        return super.getCell("GP");
    }

    /*public CsvCell getExternalUsualGPOrganisation() {
        return super.getCell("SERVICE_ID");
    }*/

    public CsvCell getPatientAction() {
        return super.getCell("ACTION");
    }

}
