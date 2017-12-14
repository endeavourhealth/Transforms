package org.endeavourhealth.transform.homerton.schema;

import org.endeavourhealth.transform.common.exceptions.TransformException;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;
import org.endeavourhealth.transform.emis.csv.schema.AbstractCsvParser;

import java.util.Date;

public class Patient extends AbstractCsvParser {

    public Patient(String version, String filePath, boolean openParser) throws Exception {
        super(version, filePath, openParser, EmisCsvToFhirTransformer.CSV_FORMAT, EmisCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD, EmisCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {

            return new String[] {
                    "PatientDimID",
                    "PersonID",
                    "CNN",
                    "NHSNo",
                    "Firstname",
                    "Surname",
                    "DOB",
                    "DOD",
                    "MobileTel",
                    "HomeTel",
                    "WorkTel",
                    "GenderID",
                    "GenderName",
                    "EthnicGroupID",
                    "EthnicGroupName",
                    "ReligionID",
                    "ReligionName",
                    "LanguageID",
                    "LanguageName",
                    "GPPersonID",
                    "GPID",
                    "GPName",
                    "PracticeOrgID",
                    "PracticeID",
                    "PracticeName",
                    "AddressLine1",
                    "AddressLine2",
                    "AddressLine3",
                    "City",
                    "County",
                    "Country",
                    "Postcode",
                    "StartDate",
                    "EndDate",
                    "CurrentFlag",
                    "CreateDate",
                    "UpdateDate",
                    "ImportID",
                    "Hash"
            };

    }

    public String getPatientDimID() {
        return super.getString("PatientDimID");
    }
    public String getPersonId() {
        return super.getString("PersonId");
    }
    public String getCNN() {
        return super.getString("CNN");
    }
    public String getNHSNo() {
        return super.getString("NHSNo");
    }

    public String getFirstname() {
        return super.getString("Firstname");
    }
    public String getSurname() {
        return super.getString("Surname");
    }
    public Date getDOB() throws TransformException {
        return super.getDate("DOB");
    }
    public Date getDOD() throws TransformException {
        return super.getDate("DOD");
    }

    public String getMobileTel() {
        return super.getString("MobileTel");
    }
    public String getHomeTel() {
        return super.getString("HomeTel");
    }
    public String getWorkTel() {
        return super.getString("WorkTel");
    }

    public int getGenderID() {
        return super.getInt("GenderID");
    }
    public String getEthnicGroupID() {
        return super.getString("EthnicGroupID");
    }
    public String getEthnicGroupName() {
        return super.getString("EthnicGroupName");
    }

    public String getGPID() {
        return super.getString("GPID");
    }
    public String getPracticeID() {
        return super.getString("PracticeID");
    }

    public String getAddressLine1() {
        return super.getString("AddressLine1");
    }
    public String getAddressLine2() {
        return super.getString("AddressLine2");
    }
    public String getAddressLine3() {
        return super.getString("AddressLine3");
    }
    public String getCity() {
        return super.getString("City");
    }
    public String getCounty() {
        return super.getString("County");
    }
    public String getPostcode() {
        return super.getString("Postcode");
    }
}
