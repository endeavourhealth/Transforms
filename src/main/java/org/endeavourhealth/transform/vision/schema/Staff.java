package org.endeavourhealth.transform.vision.schema;

import org.endeavourhealth.transform.emis.csv.schema.AbstractCsvParser;
import org.endeavourhealth.transform.vision.VisionCsvToFhirTransformer;

public class Staff extends AbstractCsvParser {

    public Staff(String version, String filePath, boolean openParser) throws Exception {
        super(version, filePath, openParser, VisionCsvToFhirTransformer.CSV_FORMAT.withHeader(
                "ID",
                "HCP_TYPE",
                "GMP_CODE",
                "SURNAME",
                "FORENAME",
                "TITLE",
                "SEX",
                "SERVICE_ID"),
                VisionCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD,
                VisionCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
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

    public String getUserID() {
        return super.getString("ID");
    }
    public String getOrganisationID() {
        return super.getString("SERVICE_ID");
    }
    public String getTitle() {
        return super.getString("TITLE");
    }
    public String getSex() {
        return super.getString("SEX");
    }
    public String getGivenName() {
        return super.getString("FORENAME");
    }
    public String getSurname() {
        return super.getString("SURNAME");
    }
    public String getGMPCode() {
        return super.getString("GMP_CODE");
    }
    public String getJobCategoryCode() {
        return super.getString("HCP_TYPE");
    }
}
