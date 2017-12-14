package org.endeavourhealth.transform.vision.schema;

import org.endeavourhealth.transform.common.exceptions.TransformException;
import org.endeavourhealth.transform.emis.csv.schema.AbstractCsvParser;
import org.endeavourhealth.transform.vision.VisionCsvToFhirTransformer;

import java.util.Date;

public class Patient extends AbstractCsvParser {

    public Patient(String version, String filePath, boolean openParser) throws Exception {
        super(version, filePath, openParser, VisionCsvToFhirTransformer.CSV_FORMAT.withHeader(
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
                VisionCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD,
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

    public String getPatientID() {
        return super.getString("PID");
    }
    public String getPatientRef() {
        return super.getString("REFERENCE");
    }
    public String getOrganisationID() { return super.getString("SERVICE_ID"); }
    public String getSex() {
        return super.getString("SEX");
    }
    public String getEthnicOrigin () { return super.getString("ETHNIC"); }
    public String getMaritalStatus () { return super.getString("MARITAL_STATUS"); }
    public Date getDateOfBirth() throws TransformException {
        return super.getDate("DATE_OF_BIRTH");
    }
    public Date getDateOfDeath() throws TransformException {
        return super.getDate("DATE_OF_DEATH");
    }
    public String getTitle() {
        return super.getString("TITLE");
    }
    public String getGivenName() {
        return super.getString("FORENAME");
    }
        public String getSurname() {
        return super.getString("SURNAME");
    }
    public Date getDateOfRegistration() throws TransformException {
        return super.getDate("REGISTERED_DATE");
    }
    public String getNhsNumber() {
        return super.getString("NHS_NUMBER");
    }
    public int getPatientNumber() {
        return super.getInt("PRACT_NUMBER");
    }
    public String getPatientTypeCode() {
        return super.getString("ACTIVE");
    }
    public String getHouseNameFlatNumber() {
        return super.getString("ADDRESS_1");
    }
    public String getNumberAndStreet() {
        return super.getString("ADDRESS_2");
    }
    public String getVillage() {
        return super.getString("ADDRESS_3");
    }
    public String getTown() {
        return super.getString("ADDRESS_4");
    }
    public String getCounty() {
        return super.getString("ADDRESS_5");
    }
    public String getPostcode() {
        return super.getString("POSTCODE");
    }
    public Date getDateOfDeactivation() throws TransformException {
        return super.getDate("REMOVED_DATE");
    }

    public String getHomePhone() {
        return super.getString("PHONE_NUMBER");
    }
    public String getMobilePhone() {
        return super.getString("MOBILE_NUMBER");
    }
    public String getEmail() {
        return super.getString("EMAIL");
    }

    public String getUsualGpID() {
        return super.getString("GP_USUAL");
    }
    public String getExternalUsualGPID() {
        return super.getString("GP");
    }
    public String getExternalUsualGPOrganisation() {
        return super.getString("SURGERY");
    }
    public String getPatientAction() {
        return super.getString("ACTION");
    }
}
