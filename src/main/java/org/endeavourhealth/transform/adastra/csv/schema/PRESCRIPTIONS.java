package org.endeavourhealth.transform.adastra.csv.schema;

import org.endeavourhealth.transform.adastra.AdastraCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;

import java.util.UUID;

public class PRESCRIPTIONS extends AbstractCsvParser {

    public PRESCRIPTIONS(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, AdastraCsvToFhirTransformer.CSV_FORMAT.withHeader(
                "CaseRef",
                "ConsultationRef",
                "DrugName",
                "Preparation",
                "Dosage",
                "Quantity"),
                AdastraCsvToFhirTransformer.DATE_FORMAT,
                AdastraCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {

        return new String[]{
                "CaseRef",
                "ConsultationRef",
                "DrugName",
                "Preparation",
                "Dosage",
                "Quantity"
        };
    }

    @Override
    protected String getFileTypeDescription() {
        return "Adastra Prescriptions file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getCaseId() { return super.getCell("CaseRef"); }
    public CsvCell getConsultationId() { return super.getCell("ConsultationRef"); }
    public CsvCell getDrugName() {return super.getCell("DrugName"); }
    public CsvCell getPreparation() {return super.getCell("Preparation"); }
    public CsvCell getDosage() { return super.getCell("Dosage"); }
    public CsvCell getQuanity() { return super.getCell("Quantity"); }
}