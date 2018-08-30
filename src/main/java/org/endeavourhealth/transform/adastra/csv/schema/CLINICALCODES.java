package org.endeavourhealth.transform.adastra.csv.schema;

import org.endeavourhealth.transform.adastra.AdastraCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;

import java.util.UUID;

public class CLINICALCODES extends AbstractCsvParser {

    public CLINICALCODES(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                "CaseRef",
                "ConsultationRef",
                "ClinicalCode",
                "Term"
        };
    }

    @Override
    protected String getFileTypeDescription() {
        return "Adastra Clinical Codes file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getCaseId() {
        return super.getCell("CaseRef");
    }

    public CsvCell getConsultationId() {
        return super.getCell("ConsultationRef");
    }

    public CsvCell getCode() {
        return super.getCell("ClinicalCode");
    }

    public CsvCell getTerm() {
        return super.getCell("Term");
    }
}
