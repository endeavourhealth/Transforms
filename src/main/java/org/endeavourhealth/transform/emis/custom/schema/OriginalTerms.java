package org.endeavourhealth.transform.emis.custom.schema;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;

import java.util.UUID;

public class OriginalTerms extends AbstractCsvParser {

    public OriginalTerms(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath, CSVFormat csvFormat, String dateFormat, String timeFormat) throws Exception {
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
                "OrganisationCdb",
                "OrganisationOds",
                "PatientGuid",
                "ObservationGuid",
                "OriginalTerm"
        };
    }

    public CsvCell getOrganisationCdb() {
        return getCell("OrganisationCdb");
    }

    public CsvCell getOrganisationOds() {
        return getCell("OrganisationOds");
    }

    public CsvCell getPatientGuid() {
        return getCell("PatientGuid");
    }

    public CsvCell getObservationGuid() {
        return getCell("ObservationGuid");
    }

    public CsvCell getOriginalTerm() {
        return getCell("OriginalTerm");
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
