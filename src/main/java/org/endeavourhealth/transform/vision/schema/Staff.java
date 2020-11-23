package org.endeavourhealth.transform.vision.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.vision.VisionCsvToFhirTransformer;

import java.util.UUID;

public class Staff extends AbstractCsvParser {

    public Staff(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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

        //the test pack has the same columns as the live data, but in a different order
        if (version.equals(VisionCsvToFhirTransformer.VERSION_TEST_PACK)) {
            return new String[]{
                    "ID",
                    "SURNAME",
                    "FORENAME",
                    "TITLE",
                    "SEX",
                    "GMP_CODE",             //NHS specified doctor number, GMP code or internal gp identifier
                    "HCP_TYPE",
                    "SERVICE_ID"            //Link to organisation/practice
            };

        } else {
            return new String[]{
                    "ID",
                    "HCP_TYPE",
                    "GMP_CODE",             //NHS specified doctor number, GMP code or internal gp identifier
                    "SURNAME",
                    "FORENAME",
                    "TITLE",
                    "SEX",
                    "SERVICE_ID"            //Link to organisation/practice
            };
        }
    }


    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getUserID() {
        return super.getCell("ID");
    }

    public CsvCell getOrganisationID() {
        return super.getCell("SERVICE_ID");
    }

    public CsvCell getTitle() {
        return super.getCell("TITLE");
    }

    public CsvCell getSex() {
        return super.getCell("SEX");
    }

    public CsvCell getGivenName() {
        return super.getCell("FORENAME");
    }

    public CsvCell getSurname() {
        return super.getCell("SURNAME");
    }

    public CsvCell getGMPCode() {
        return super.getCell("GMP_CODE");
    }

    public CsvCell getJobCategoryCode() {
        return super.getCell("HCP_TYPE");
    }
}
