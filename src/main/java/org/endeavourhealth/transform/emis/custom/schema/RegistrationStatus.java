package org.endeavourhealth.transform.emis.custom.schema;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;

import java.util.UUID;

public class RegistrationStatus extends AbstractCsvParser {

    public RegistrationStatus(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
        super(serviceId,
                systemId,
                exchangeId,
                version,
                filePath,
                csvFormat,
                dateFormat,
                timeFormat);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return getHeaders();
    }

    /**
     * the file doesn't contain headers, so we just have to specify what they'll be
     */
    private static String[] getHeaders() {
        return new String[] {
                "OrganisationGuid",
                "PatientGuid",
                "Date",
                "RegistrationStatus",
                "RegistrationType",
                "ProcessingOrder"
        };
    }

    public CsvCell getOrganisationGuid() {
        return getCell("OrganisationGuid");
    }

    public CsvCell getPatientGuid() {
        return getCell("PatientGuid");
    }

    public CsvCell getDate() {
        return getCell("Date");
    }

    public CsvCell getRegistrationStatus() {
        return getCell("RegistrationStatus");
    }

    public CsvCell getRegistrationType() {
        return getCell("RegistrationType");
    }

    public CsvCell getProcessingOrder() {
        return getCell("ProcessingOrder");
    }

    @Override
    protected String getFileTypeDescription() {
        return "Bespoke Emis registration status extract";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}