package org.endeavourhealth.transform.adastra.csv.schema;

import org.endeavourhealth.transform.adastra.AdastraCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;

import java.util.UUID;

public class PRESCRIPTIONS extends AbstractCsvParser {

    public PRESCRIPTIONS(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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

        if (version.equalsIgnoreCase(AdastraCsvToFhirTransformer.VERSION_1)) {

            return new String[]{
                    "CaseRef",
                    "ConsultationRef",
                    "DrugName",
                    "Preparation",
                    "Dosage",
                    "Quantity"
            };
        } else {

            return new String[]{
                    "CaseRef",
                    "ConsultationRef",
                    "DrugName",
                    "Preparation",
                    "Dosage",
                    "Quantity",
                    "DMDCode",
                    "Issue"
            };
        }
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

    public CsvCell getDrugName() {
        return super.getCell("DrugName");
    }

    public CsvCell getPreparation() {
        return super.getCell("Preparation");
    }

    public CsvCell getDosage() {
        return super.getCell("Dosage");
    }

    public CsvCell getQuanity() {
        return super.getCell("Quantity");
    }

    //version 2 additional

    public CsvCell getDMDCode() {
        return super.getCell("DMDCode");
    }

    public CsvCell getIssue() {
        return super.getCell("Issue");
    }
}
