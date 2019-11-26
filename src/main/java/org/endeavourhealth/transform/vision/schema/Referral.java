package org.endeavourhealth.transform.vision.schema;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.vision.VisionCsvToFhirTransformer;

import java.util.UUID;

public class Referral extends AbstractCsvParser {

    public Referral(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
        //the test pack doesn't contain any referral data, so we don't know what columns it would contain
        return new String[]{
                "PID",
                "ID",
                "DATE",
                "HCP",
                "HCP_TYPE",
                "TO_HCP",
                "SPECIALTY",
                "UNIT",
                "TYPE",
                "CONTRACTOR",
                "CONTRACT",
                "ACTION_DATE",
                "LINKS",
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
    public CsvCell getReferralID() {
        return super.getCell("ID");
    }
    public CsvCell getReferralDate() throws TransformException {
        return super.getCell("DATE");
    }
    public CsvCell getActionDate() throws TransformException {
        return super.getCell("ACTION_DATE");
    }
    public CsvCell getReferralUserID() {
        return super.getCell("HCP");
    }
    public CsvCell getReferralUserType() {
        return super.getCell("HCP_TYPE");
    }
    public CsvCell getReferralDestOrgID() {
        return super.getCell("TO_HCP");
    }
    public CsvCell getOrganisationID() {
        return super.getCell("SERVICE_ID");
    }
    public CsvCell getAction() {
        return super.getCell("ACTION");
    }
    public CsvCell getReferralType() {
        return super.getCell("TYPE");
    }
    public CsvCell getLinks() {
        return super.getCell("LINKS");
    }

//    public String getPatientID() {
//        return super.getString("PID");
//    }
//    public String getReferralID() {
//        return super.getString("ID");
//    }
//    public Date getReferralDate() throws TransformException {
//        return super.getDate("DATE");
//    }
//    public Date getActionDate() throws TransformException {
//        return super.getDate("ACTION_DATE");
//    }
//    public String getReferralUserID() {
//        return cleanUserId(super.getString("HCP"));
//    }
//    public String getReferralUserType() {
//        return super.getString("HCP_TYPE");
//    }
//    public String getReferralDestOrgID() {
//        return super.getString("TO_HCP");
//    }
//    public String getOrganisationID() {
//        return super.getString("SERVICE_ID");
//    }
//    public String getAction() {
//        return super.getString("ACTION");
//    }
//    public String getReferralType() {
//        return super.getString("TYPE");
//    }
//    public String getLinks() {
//        return super.getString("LINKS");
//    }

}
