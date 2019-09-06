package org.endeavourhealth.transform.emis.custom.schema;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;

import java.util.UUID;

public class RegistrationStatus extends AbstractCsvParser {

    public static final String VERSION_WITH_PROCESSING_ID = "WithProcessingId";
    //public static final String VERSION_WITHOUT_PROCESSING_ID = "WithoutProcessingId"; //processing ID is required for accurate sorting

    public RegistrationStatus(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath, CSVFormat csvFormat, String dateFormat, String timeFormat) {
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
        return getHeaders(version);
    }

    /**
     * the file doesn't contain headers, so we just have to specify what they'll be
     */
    private static String[] getHeaders(String version) {
        if (version.equals(VERSION_WITH_PROCESSING_ID)) {
            return new String[] {
                    "OrganisationGuid",
                    "PatientGuid",
                    "Date",
                    "RegistrationStatus",
                    "RegistrationType",
                    "ProcessingOrder"
            };
        /*} else if (version.equals(VERSION_WITHOUT_PROCESSING_ID)) {
            return new String[] {
                    "OrganisationGuid",
                    "PatientGuid",
                    "Date",
                    "RegistrationStatus",
                    "RegistrationType"
            };*/
        } else {
            throw new RuntimeException("Unknown version [" + version + "]");
        }
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

    /**
     * note this column doesn't exist on the latest version of the file
     */
    public CsvCell getProcessingOrder() {
        return getCell("ProcessingOrder");
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
