package org.endeavourhealth.transform.emis.csv.schema.bespoke;

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
                csvFormat.withHeader(getHeaders()), //unlike other CSV files this one doesn't have headers in it
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
                "RegistrationStatusId",
                "RegistrationTypeId"
        };
    }

    public CsvCell getOrganisationGuid() {
        return getCell("OrganisationGuid");
    }

    public CsvCell getPatientGuid() {
        return getCell("PatientGuid");
    }

    public CsvCell getRegistrationStatusId() {
        return getCell("RegistrationStatusId");
    }

    public CsvCell getRegistrationTypeId() {
        return getCell("RegistrationTypeId");
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
