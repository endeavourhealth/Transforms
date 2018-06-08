package org.endeavourhealth.transform.adastra.csv.schema;

import org.endeavourhealth.transform.adastra.AdastraCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;

import java.util.UUID;

public class CONSULTATION extends AbstractCsvParser {

    public CONSULTATION(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, AdastraCsvToFhirTransformer.CSV_FORMAT.withHeader(
                "CaseRef",
                "ConsultationRef",
                "StartDateTime",
                "EndDateTime",
                "Location",
                "CaseType",
                "History",
                "Examination",
                "Diagnosis",
                "TreatmentPlan",
                "PatientName",
                "PatientForename",
                "PatientSurname",
                "ProviderType",
                "GMC"),
                AdastraCsvToFhirTransformer.DATE_FORMAT,
                AdastraCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {

        return new String[]{
                "CaseRef",
                "ConsultationRef",
                "StartDateTime",
                "EndDateTime",
                "Location",
                "CaseType",
                "History",
                "Examination",
                "Diagnosis",
                "TreatmentPlan",
                "PatientName",
                "PatientForename",
                "PatientSurname",
                "ProviderType",
                "GMC"
        };
    }

    @Override
    protected String getFileTypeDescription() {
        return "Adastra Consultation file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getCaseId() { return super.getCell("CaseRef"); }
    public CsvCell getConsultationId() { return super.getCell("ConsultationRef"); }
    public CsvCell getStartDateTime() {return super.getCell("StartDateTime"); }
    public CsvCell getEndDateTime() {return super.getCell("StartEndTime"); }
    public CsvCell getLocation() { return super.getCell("Location"); }
    public CsvCell getCaseType() { return super.getCell("CaseType"); }
    public CsvCell getHistory() { return super.getCell("History"); }
    public CsvCell getExamination() { return super.getCell("Examination"); }
    public CsvCell getDiagnosis() { return super.getCell("Diagnosis"); }
    public CsvCell getTreatmentPlan() { return super.getCell("TreatmentPlan"); }
    public CsvCell getPatientName() { return super.getCell("PatientName"); }
    public CsvCell getPatientForename() { return super.getCell("PatientForename"); }
    public CsvCell getPatientSurname() { return super.getCell("PatientSurname"); }
    public CsvCell getProviderType() { return super.getCell("ProviderType"); }
    public CsvCell getGMC() { return super.getCell("GMC"); }
}
