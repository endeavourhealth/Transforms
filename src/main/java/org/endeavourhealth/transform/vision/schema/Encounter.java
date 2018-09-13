package org.endeavourhealth.transform.vision.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.vision.VisionCsvToFhirTransformer;

import java.util.UUID;

public class Encounter extends AbstractCsvParser {

    public Encounter(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, VisionCsvToFhirTransformer.CSV_FORMAT.withHeader(
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
                VisionCsvToFhirTransformer.DATE_FORMAT,
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

//    public String getPatientID() {
//        return super.getString("PID");
//    }
//    public String getConsultationID() {
//        return super.getString("ID");
//    }
//
//    public String getOrganisationID() {
//        return super.getString("SERVICE_ID");
//    }
//    public Date getEffectiveDate() throws TransformException {
//        return super.getDate("DATE");
//    }
//
//    public Date getEnteredDate() throws TransformException {
//        return super.getDate("DATE");
//    }
//    public Date getEnteredDateTime() throws TransformException {
//        return super.getDateTime("DATE", "TIME");
//    }
//
//    public String getClinicianUserID() {
//        return cleanUserId(super.getString("HCP"));
//    }
//
//    public String getConsultationSessionTypeCode() { return super.getString("SESSION"); }
//
//    public String getConsultationLocationTypeCode() { return super.getString("LOCATION"); }
//
//    public String getAction () { return super.getString("ACTION");
//    }

}
