package org.endeavourhealth.transform.emis.csv.schema.admin;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;

import java.util.UUID;

public class Patient extends AbstractCsvParser {

    public Patient(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, EmisCsvToFhirTransformer.CSV_FORMAT, EmisCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD, EmisCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {

        //EMIS test packs spell two of the columns with "Ususal" rather than "Usual", so handling that variation
        if (version.equals(EmisCsvToFhirTransformer.VERSION_5_0)
                || version.equals(EmisCsvToFhirTransformer.VERSION_5_1)) {
            return new String[] {
                    "PatientGuid",
                    "OrganisationGuid",
                    "UsualGpUserInRoleGuid",
                    "Sex",
                    "DateOfBirth",
                    "DateOfDeath",
                    "Title",
                    "GivenName",
                    "MiddleNames",
                    "Surname",
                    "DateOfRegistration",
                    "NhsNumber",
                    "PatientNumber",
                    "PatientTypeDescription",
                    "DummyType",
                    "HouseNameFlatNumber",
                    "NumberAndStreet",
                    "Village",
                    "Town",
                    "County",
                    "Postcode",
                    "ResidentialInstituteCode",
                    "NHSNumberStatus",
                    "CarerName",
                    "CarerRelation",
                    "PersonGuid",
                    "DateOfDeactivation",
                    "Deleted",
                    "SpineSensitive",
                    "IsConfidential",
                    "EmailAddress",
                    "HomePhone",
                    "MobilePhone",
                    "ExternalUsualGPGuid",
                    "ExternalUsusalGP",
                    "ExternalUsusalGPOrganisation",
                    "ProcessingId"
            };
        } else {
            return new String[] {
                    "PatientGuid",
                    "OrganisationGuid",
                    "UsualGpUserInRoleGuid",
                    "Sex",
                    "DateOfBirth",
                    "DateOfDeath",
                    "Title",
                    "GivenName",
                    "MiddleNames",
                    "Surname",
                    "DateOfRegistration",
                    "NhsNumber",
                    "PatientNumber",
                    "PatientTypeDescription",
                    "DummyType",
                    "HouseNameFlatNumber",
                    "NumberAndStreet",
                    "Village",
                    "Town",
                    "County",
                    "Postcode",
                    "ResidentialInstituteCode",
                    "NHSNumberStatus",
                    "CarerName",
                    "CarerRelation",
                    "PersonGuid",
                    "DateOfDeactivation",
                    "Deleted",
                    "SpineSensitive",
                    "IsConfidential",
                    "EmailAddress",
                    "HomePhone",
                    "MobilePhone",
                    "ExternalUsualGPGuid",
                    "ExternalUsualGP",
                    "ExternalUsualGPOrganisation",
                    "ProcessingId"
            };
        }

    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getPatientGuid() {
        return super.getCell("PatientGuid");
    }
    public CsvCell getOrganisationGuid() {
        return super.getCell("OrganisationGuid");
    }
    public CsvCell getUsualGpUserInRoleGuid() {
        return super.getCell("UsualGpUserInRoleGuid");
    }
    public CsvCell getSex() {
        return super.getCell("Sex");
    }
    public CsvCell getDateOfBirth() {
        return super.getCell("DateOfBirth");
    }
    public CsvCell getDateOfDeath() {
        return super.getCell("DateOfDeath");
    }
    public CsvCell getTitle() {
        return super.getCell("Title");
    }
    public CsvCell getGivenName() {
        return super.getCell("GivenName");
    }
    public CsvCell getMiddleNames() {
        return super.getCell("MiddleNames");
    }
    public CsvCell getSurname() {
        return super.getCell("Surname");
    }
    public CsvCell getDateOfRegistration() throws TransformException {
        return super.getCell("DateOfRegistration");
    }
    public CsvCell getNhsNumber() {
        return super.getCell("NhsNumber");
    }
    public CsvCell getPatientNumber() {
        return super.getCell("PatientNumber");
    }
    public CsvCell getPatientTypeDescription() {
        return super.getCell("PatientTypeDescription");
    }
    public CsvCell getDummyType() {
        return super.getCell("DummyType");
    }
    public CsvCell getHouseNameFlatNumber() {
        return super.getCell("HouseNameFlatNumber");
    }
    public CsvCell getNumberAndStreet() {
        return super.getCell("NumberAndStreet");
    }
    public CsvCell getVillage() {
        return super.getCell("Village");
    }
    public CsvCell getTown() {
        return super.getCell("Town");
    }
    public CsvCell getCounty() {
        return super.getCell("County");
    }
    public CsvCell getPostcode() {
        return super.getCell("Postcode");
    }
    public CsvCell getResidentialInstituteCode() {
        return super.getCell("ResidentialInstituteCode");
    }
    public CsvCell getNHSNumberStatus() {
        return super.getCell("NHSNumberStatus");
    }
    public CsvCell getCarerName() {
        return super.getCell("CarerName");
    }
    public CsvCell getCarerRelation() {
        return super.getCell("CarerRelation");
    }
    public CsvCell getPersonGUID() {
        return super.getCell("PersonGuid");
    }
    public CsvCell getDateOfDeactivation() {
        return super.getCell("DateOfDeactivation");
    }
    public CsvCell getDeleted() {
        return super.getCell("Deleted");
    }
    public CsvCell getSpineSensitive() {
        return super.getCell("SpineSensitive");
    }
    public CsvCell getIsConfidential() {
        return super.getCell("IsConfidential");
    }
    public CsvCell getEmailAddress() {
        return super.getCell("EmailAddress");
    }
    public CsvCell getHomePhone() {
        return super.getCell("HomePhone");
    }
    public CsvCell getMobilePhone() {
        return super.getCell("MobilePhone");
    }
    public CsvCell getExternalUsualGPGuid() {
        return super.getCell("ExternalUsualGPGuid");
    }

    /**
     * free-text name of the above person, but only occasionally populated
     */
    public CsvCell getExternalUsualGP() {
        return super.getCell("ExternalUsualGP");
    }
    public CsvCell getExternalUsualGPOrganisation() {
        return super.getCell("ExternalUsualGPOrganisation");
    }
    public CsvCell getProcessingId() {
        return super.getCell("ProcessingId");
    }

    /**
     * special function to handle mis-named columns in test pack
     */
    public CsvCell getExternalUsusalGP() {
        return super.getCell("ExternalUsusalGP");
    }
    public CsvCell getExternalUsusalGPOrganisation() {
        return super.getCell("ExternalUsusalGPOrganisation");
    }


}
