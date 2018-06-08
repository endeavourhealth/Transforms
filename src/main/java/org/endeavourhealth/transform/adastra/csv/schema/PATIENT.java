package org.endeavourhealth.transform.adastra.csv.schema;

import org.endeavourhealth.transform.adastra.AdastraCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;

import java.util.UUID;

public class PATIENT extends AbstractCsvParser {

    public PATIENT(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, AdastraCsvToFhirTransformer.CSV_FORMAT.withHeader(
                "CaseRef",
                "PatientRef",
                "Forename",
                "Surname",
                "DOB",
                "NHSNumber",
                "NHSNoTraceStatus",
                "Language",
                "Ethnicity",
                "Gender",
                "RegistrationType",
                "HomeAddressType",
                "HomeAddressBuilding",
                "HomeAddressStreet",
                "HomeAddressTown",
                "HomeAddressLocality",
                "HomeAddressPostcode",
                "CurrentAddressType",
                "CurrentAddressBuilding",
                "CurrentAddressStreet",
                "CurrentAddressTown",
                "CurrentAddressLocality",
                "CurrentAddressPostcode",
                "MobilePhone",
                "HomePhone",
                "OtherPhone"),
                AdastraCsvToFhirTransformer.DATE_FORMAT,
                AdastraCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {

        return new String[]{
                "CaseRef",
                "PatientRef",
                "Forename",
                "Surname",
                "DOB",
                "NHSNumber",
                "NHSNoTraceStatus",
                "Language",
                "Ethnicity",
                "Gender",
                "RegistrationType",
                "HomeAddressType",
                "HomeAddressBuilding",
                "HomeAddressStreet",
                "HomeAddressTown",
                "HomeAddressLocality",
                "HomeAddressPostcode",
                "CurrentAddressType",
                "CurrentAddressBuilding",
                "CurrentAddressStreet",
                "CurrentAddressTown",
                "CurrentAddressLocality",
                "CurrentAddressPostcode",
                "MobilePhone",
                "HomePhone",
                "OtherPhone"
        };
    }

    @Override
    protected String getFileTypeDescription() {
        return "Adastra Patient file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getCaseId() { return super.getCell("CaseRef"); }
    public CsvCell getPatientId() { return super.getCell("PatientRef"); }
    public CsvCell getForename() { return super.getCell("Forename"); }
    public CsvCell getSurname() { return super.getCell("Surname"); }
    public CsvCell getDOB() { return super.getCell("DOB"); }
    public CsvCell getNHSNumber() { return super.getCell("NHSNumber"); }
    public CsvCell getNHSNoTraceStatus() { return super.getCell("NHSNoTraceStatus"); }
    public CsvCell getLanguage() { return super.getCell("Language"); }
    public CsvCell getEthnicity() { return super.getCell("Ethnicity"); }
    public CsvCell getGender() { return super.getCell("Gender"); }
    public CsvCell getRegistrationType() { return super.getCell("RegistrationType"); }
    public CsvCell getHomeAddressType() { return super.getCell("HomeAddressType"); }
    public CsvCell getHomeAddressBuilding() { return super.getCell("HomeAddressBuilding"); }
    public CsvCell getHomeAddressStreet() { return super.getCell("HomeAddressStreet"); }
    public CsvCell getHomeAddressTown() { return super.getCell("HomeAddressTown"); }
    public CsvCell getHomeAddressLocality() { return super.getCell("HomeAddressLocality"); }
    public CsvCell getHomeAddressPostcode() { return super.getCell("HomeAddressPostcode"); }
    public CsvCell getCurrentAddressType() { return super.getCell("CurrentAddressType"); }
    public CsvCell getCurrentAddressBuilding() { return super.getCell("CurrentAddressBuilding"); }
    public CsvCell getCurrentAddressStreet() { return super.getCell("CurrentAddressStreet"); }
    public CsvCell getCurrentAddressTown() { return super.getCell("CurrentAddressTown"); }
    public CsvCell getCurrentAddressLocality() { return super.getCell("CurrentAddressLocality"); }
    public CsvCell getCurrentAddressPostcode() { return super.getCell("CurrentAddressPostcode"); }
    public CsvCell getMobilePhone() { return super.getCell("MobilePhone"); }
    public CsvCell getHomePhone() { return super.getCell("HomePhone"); }
    public CsvCell getOtherPhone() { return super.getCell("OtherPhone"); }
}
