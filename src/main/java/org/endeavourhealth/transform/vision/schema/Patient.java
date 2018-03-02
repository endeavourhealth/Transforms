package org.endeavourhealth.transform.vision.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.vision.VisionCsvToFhirTransformer;

import java.util.UUID;

public class Patient extends AbstractCsvParser {

    public Patient(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, VisionCsvToFhirTransformer.CSV_FORMAT.withHeader(
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
                "ACTION"),
                VisionCsvToFhirTransformer.DATE_FORMAT,
                VisionCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {

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

    @Override
    protected String getFileTypeDescription() {
        return "Vision patient file";
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
    public CsvCell getOrganisationID() { return super.getCell("SERVICE_ID"); }
    public CsvCell getSex() {
        return super.getCell("SEX");
    }
    public CsvCell getEthnicOrigin () { return super.getCell("ETHNIC"); }
    public CsvCell getMaritalStatus () { return super.getCell("MARITAL_STATUS"); }
    public CsvCell getDateOfBirth() {return super.getCell("DATE_OF_BIRTH"); }
    public CsvCell getDateOfDeath() {return super.getCell("DATE_OF_DEATH"); }
    public CsvCell getTitle() {
        return super.getCell("TITLE");
    }
    public CsvCell getGivenName() {return super.getCell("FORENAME");}
    public CsvCell getSurname() {
        return super.getCell("SURNAME");
    }
    public CsvCell getDateOfRegistration() {return super.getCell("REGISTERED_DATE"); }
    public CsvCell getNhsNumber() {
        return super.getCell("NHS_NUMBER");
    }
    public CsvCell getPatientNumber() {
        return super.getCell("PRACT_NUMBER");
    }
    public CsvCell getPatientTypeCode() {
        return super.getCell("ACTIVE");
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
    public CsvCell getDateOfDeactivation() { return super.getCell("REMOVED_DATE"); }
    public CsvCell getHomePhone() {
        return super.getCell("PHONE_NUMBER");
    }
    public CsvCell getMobilePhone() {
        return super.getCell("MOBILE_NUMBER");
    }
    public CsvCell getEmail() {
        return super.getCell("EMAIL");
    }
    public CsvCell getUsualGpID() {
        return super.getCell("GP_USUAL");
    }
    public CsvCell getExternalUsualGPID() { return super.getCell("GP"); }
    public CsvCell getExternalUsualGPOrganisation() {
        return super.getCell("SERVICE_ID");
    }
    public CsvCell getPatientAction() {
        return super.getCell("ACTION");
    }

//    public String getPatientID() {
//        return super.getString("PID");
//    }
//    public String getPatientRef() {
//        return super.getString("REFERENCE");
//    }
//    public String getOrganisationID() { return super.getString("SERVICE_ID"); }
//    public String getSex() {
//        return super.getString("SEX");
//    }
//    public String getEthnicOrigin () { return super.getString("ETHNIC"); }
//    public String getMaritalStatus () { return super.getString("MARITAL_STATUS"); }
//    public Date getDateOfBirth() throws TransformException {
//        return super.getDate("DATE_OF_BIRTH");
//    }
//    public Date getDateOfDeath() throws TransformException {
//        return super.getDate("DATE_OF_DEATH");
//    }
//    public String getTitle() {
//        return super.getString("TITLE");
//    }
//    public String getGivenName() {
//        return super.getString("FORENAME");
//    }
//    public String getSurname() {
//        return super.getString("SURNAME");
//    }
//    public Date getDateOfRegistration() throws TransformException {
//        return super.getDate("REGISTERED_DATE");
//    }
//    public String getNhsNumber() {
//        return super.getString("NHS_NUMBER");
//    }
//    public int getPatientNumber() {
//        return super.getInt("PRACT_NUMBER");
//    }
//    public String getPatientTypeCode() {
//        return super.getString("ACTIVE");
//    }
//    public String getHouseNameFlatNumber() {
//        return super.getString("ADDRESS_1");
//    }
//    public String getNumberAndStreet() {
//        return super.getString("ADDRESS_2");
//    }
//    public String getVillage() {
//        return super.getString("ADDRESS_3");
//    }
//    public String getTown() {
//        return super.getString("ADDRESS_4");
//    }
//    public String getCounty() {
//        return super.getString("ADDRESS_5");
//    }
//    public String getPostcode() {
//        return super.getString("POSTCODE");
//    }
//    public Date getDateOfDeactivation() throws TransformException {
//        return super.getDate("REMOVED_DATE");
//    }
//
//    public String getHomePhone() {
//        return super.getString("PHONE_NUMBER");
//    }
//    public String getMobilePhone() {
//        return super.getString("MOBILE_NUMBER");
//    }
//    public String getEmail() {
//        return super.getString("EMAIL");
//    }
//
//    public String getUsualGpID() {
//        return super.getString("GP_USUAL");
//    }
//    public String getExternalUsualGPID() {
//        return super.getString("GP");
//    }
//    public String getExternalUsualGPOrganisation() {
//        return super.getString("SERVICE_ID");
//    }
//    public String getPatientAction() {
//        return super.getString("ACTION");
//    }
}
