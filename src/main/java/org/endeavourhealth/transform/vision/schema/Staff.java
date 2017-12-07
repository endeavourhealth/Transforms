package org.endeavourhealth.transform.vision.schema;

import org.endeavourhealth.transform.emis.csv.schema.AbstractCsvParser;
import org.endeavourhealth.transform.vision.VisionCsvToFhirTransformer;

import java.io.File;

public class Staff extends AbstractCsvParser {

    public Staff(String version, File f, boolean openParser) throws Exception {
        super(version, f, openParser, VisionCsvToFhirTransformer.CSV_FORMAT.withHeader(
                "ID",
                "SURNAME",
                "FORENAME",
                "TITLE",
                "SEX",
                "HCP",
                "HCP_TYPE",
                "SERVICE_ID"),
                VisionCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD,
                VisionCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "ID",
                "SURNAME",
                "FORENAME",
                "TITLE",
                "SEX",
                "HCP",                  //NHS specified doctor number, GMP code or internal gp identifier
                "HCP_TYPE",
                "SERVICE_ID"            //Link to organisation/practice
        };
    }

    public String getUserID() {
        return super.getString("HCP");
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
    public String getJobCategoryCode() {
        return super.getString("HCP_TYPE");
    }
}
