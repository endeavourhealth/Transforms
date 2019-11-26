package org.endeavourhealth.transform.vision.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.vision.VisionCsvToFhirTransformer;

import java.util.UUID;

public class Encounter extends AbstractCsvParser {

    public Encounter(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                VisionCsvToFhirTransformer.CSV_FORMAT.withHeader(getHeaders(version)),
                VisionCsvToFhirTransformer.DATE_FORMAT,
                VisionCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return getHeaders(version);
    }

    private static String[] getHeaders(String version) {
        if (version.equals(VisionCsvToFhirTransformer.VERSION_TEST_PACK)) {
            //only difference from live data is that HCP code (e.g. GMC number) replaces the ID (i.e. number)
            return new String[]{
                    "PID",
                    "ID",
                    "DATE",
                    "HCP_CODE", //e.g. Gxxxxxx
                    "HCP_TYPE",
                    "SESSION",
                    "LOCATION",
                    "TIME",
                    "DURATION",
                    "TRAVEL",
                    "LINKS",
                    "PRACT_NUMBER",
                    "SERVICE_ID",
                    "ACTION"
            };

        } else {
            return new String[]{
                    "PID",
                    "ID",
                    "DATE",
                    "HCP",
                    "HCP_TYPE",
                    "SESSION",
                    "LOCATION",
                    "TIME",
                    "DURATION",
                    "TRAVEL",   //not supported
                    "LINKS",
                    "PRACT_NUMBER",
                    "SERVICE_ID",
                    "ACTION"
            };
        }
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getPatientID() {
        return super.getCell("PID");
    }

    public CsvCell getConsultationID() {
        return super.getCell("ID");
    }

    public CsvCell getOrganisationID() {
        return super.getCell("SERVICE_ID");
    }

    public CsvCell getEffectiveDate() {
        return super.getCell("DATE");
    }

    public CsvCell getEffectiveTime() {
        return super.getCell("TIME");
    }

    public CsvCell getClinicianUserID() {
        return super.getCell("HCP");
    }

    public CsvCell getConsultationSessionTypeCode() {
        return super.getCell("SESSION");
    }

    public CsvCell getConsultationLocationTypeCode() {
        return super.getCell("LOCATION");
    }

    public CsvCell getAction() {
        return super.getCell("ACTION");
    }



}
