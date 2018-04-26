package org.endeavourhealth.transform.homerton.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.homerton.HomertonCsvToFhirTransformer;

import java.util.UUID;

public class PatientTable extends AbstractCsvParser {

    public PatientTable(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, HomertonCsvToFhirTransformer.CSV_FORMAT, HomertonCsvToFhirTransformer.DATE_FORMAT, HomertonCsvToFhirTransformer.TIME_FORMAT);
    }

    //@Override
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

    @Override
    protected String getFileTypeDescription() {
        return "Homerton patient file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getPatientDimID() {
        return super.getCell("PatientDimID");
    }
    public CsvCell getPersonId() {
        return super.getCell("PersonID");
    }
    public CsvCell getCNN() {
        return super.getCell("CNN");
    }
    public CsvCell getNHSNo() {
        return super.getCell("NHSNo");
    }

    public CsvCell getFirstname() {
        return super.getCell("Firstname");
    }
    public CsvCell getSurname() {
        return super.getCell("Surname");
    }
    public CsvCell getDOB() {
        return super.getCell("DOB");
    }
    public CsvCell getDOD() {
        return super.getCell("DOD");
    }

    public CsvCell getMobileTel() {
        return super.getCell("MobileTel");
    }
    public CsvCell getHomeTel() {
        return super.getCell("HomeTel");
    }
    public CsvCell getWorkTel() {
        return super.getCell("WorkTel");
    }

    public CsvCell getGenderID() {
        return super.getCell("GenderID");
    }
    public CsvCell getEthnicGroupID() {
        return super.getCell("EthnicGroupID");
    }
    public CsvCell getEthnicGroupName() {
        return super.getCell("EthnicGroupName");
    }

    public CsvCell getGPID() {
        return super.getCell("GPID");
    }
    public CsvCell getPracticeID() {
        return super.getCell("PracticeID");
    }

    public CsvCell getAddressLine1() {
        return super.getCell("AddressLine1");
    }
    public CsvCell getAddressLine2() {
        return super.getCell("AddressLine2");
    }
    public CsvCell getAddressLine3() {
        return super.getCell("AddressLine3");
    }
    public CsvCell getCity() {
        return super.getCell("City");
    }
    public CsvCell getCounty() {
        return super.getCell("County");
    }
    public CsvCell getPostcode() {
        return super.getCell("Postcode");
    }

    /*public String getPatientDimID() {
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
    }*/
}
