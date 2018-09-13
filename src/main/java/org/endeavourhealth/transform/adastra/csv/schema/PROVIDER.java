package org.endeavourhealth.transform.adastra.csv.schema;

import org.endeavourhealth.transform.adastra.AdastraCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;

import java.util.UUID;

public class PROVIDER extends AbstractCsvParser {

    public PROVIDER(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                AdastraCsvToFhirTransformer.CSV_FORMAT.withHeader(getExpectedCsvHeaders(version)),
                AdastraCsvToFhirTransformer.DATE_FORMAT,
                AdastraCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return getExpectedCsvHeaders(version);
    }

    private static String[] getExpectedCsvHeaders(String version) {
        return new String[]{
                "PatientRef",
                "RegistrationStatus",
                "GPNationalCode",
                "GPName",
                "GPPracticeNatCode",
                "GPPracticeName",
                "GPPracticePostcode",
                "CaseRef"
        };
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getPatientId() {
        return super.getCell("PatientRef");
    }

    public CsvCell getRegistrationStatus() {
        return super.getCell("RegistrationStatus");
    }

    public CsvCell getGPNationalCode() {
        return super.getCell("GPNationalCode");
    }

    public CsvCell getGPName() {
        return super.getCell("GPName");
    }

    public CsvCell getGPPracticeNatCode() {
        return super.getCell("GPPracticeNatCode");
    }

    public CsvCell getGPPracticeName() {
        return super.getCell("GPPracticeName");
    }

    public CsvCell getGPPracticePostcode() {
        return super.getCell("GPPracticePostcode");
    }

    public CsvCell getCaseId() {
        return super.getCell("CaseRef");
    }
}