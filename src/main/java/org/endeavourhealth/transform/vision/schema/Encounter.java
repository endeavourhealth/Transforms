package org.endeavourhealth.transform.vision.schema;

import org.endeavourhealth.transform.common.exceptions.TransformException;
import org.endeavourhealth.transform.emis.csv.schema.AbstractCsvParser;
import org.endeavourhealth.transform.vision.VisionCsvToFhirTransformer;

import java.io.File;
import java.util.Date;

import static org.endeavourhealth.transform.vision.VisionCsvToFhirTransformer.cleanUserId;

public class Encounter extends AbstractCsvParser {

    public Encounter(String version, File f, boolean openParser) throws Exception {
        super(version, f, openParser, VisionCsvToFhirTransformer.CSV_FORMAT.withHeader(
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
                "ACTION"),
                VisionCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD,
                VisionCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {

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

    public String getPatientID() {
        return super.getString("PID");
    }
    public String getConsultationID() {
        return super.getString("ID");
    }

    public String getOrganisationID() {
        return super.getString("SERVICE_ID");
    }
    public Date getEffectiveDate() throws TransformException {
        return super.getDate("DATE");
    }

    public Date getEnteredDate() throws TransformException {
        return super.getDate("DATE");
    }
    public Date getEnteredDateTime() throws TransformException {
        return super.getDateTime("DATE", "TIME");
    }

    public String getClinicianUserID() {
        return cleanUserId(super.getString("HCP"));
    }

    public String getConsultationSessionTypeCode() { return super.getString("SESSION"); }

    public String getConsultationLocationTypeCode() { return super.getString("LOCATION"); }

    public String getAction () { return super.getString("ACTION");
    }

}
