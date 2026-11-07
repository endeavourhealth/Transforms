package org.endeavourhealth.transform.emis.csv.schema.admin;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;

import java.util.UUID;

public class PatientHistory extends AbstractCsvParser {

    public PatientHistory(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, EmisCsvToFhirTransformer.CSV_FORMAT, EmisCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD, EmisCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[] {
                "PatientGuid",
                "OrganisationGuid",
                "HistoryDate",
                "HistoryTime",
                "StatusDescription",
                "PatientTypeDescription",
                "ProcessingId"
        };
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
    public CsvCell getHistoryDate() {
        return super.getCell("HistoryDate");
    }
    public CsvCell getHistoryTime() {
        return super.getCell("HistoryTime");
    }
    public CsvCell getStatusDescription() {
        return super.getCell("StatusDescription");
    }
    public CsvCell getPatientTypeDescription() {
        return super.getCell("PatientTypeDescription");
    }
    public CsvCell getProcessingId() {
        return super.getCell("ProcessingId");
    }

}
