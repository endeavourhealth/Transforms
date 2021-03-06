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

    /**
     * Identity of the healthcare professional responsible for making referral
     */
    public CsvCell getReferralSenderUserId() {
        return super.getCell("HCP");
    }

    /**
     * Profession of the HCP responsible for making referral type.
     * Should be derived within the Respondent System from the identifier of the person involved.
     */
    public CsvCell getReferralUserType() {
        return super.getCell("HCP_TYPE");
    }

    /**
     * Healthcare professional to whom referral is made.
     */
    public CsvCell getReferralRecipientUserId() {
        return super.getCell("TO_HCP");
    }
    public CsvCell getSpecialty() {
        return super.getCell("SPECIALTY");
    }
    public CsvCell getUnit() {
        return super.getCell("UNIT");
    }

    /**
     * Type of referral or resource use requested.
         O	OPD
         A	Admission
         D	Day-case
         I	Investigation
         V 	Domiciliary visit
     */
    public CsvCell getReferralType() {
        return super.getCell("TYPE");
    }
    public CsvCell getContractor() {
        return super.getCell("CONTRACTOR");
    }
    public CsvCell getContract() {
        return super.getCell("CONTRACT");
    }
    public CsvCell getActionDate() throws TransformException {
        return super.getCell("ACTION_DATE");
    }
    public CsvCell getLinks() {
        return super.getCell("LINKS");
    }
    public CsvCell getOrganisationID() {
        return super.getCell("SERVICE_ID");
    }
    public CsvCell getAction() {
        return super.getCell("ACTION");
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
//    public String getReferralSenderUserId() {
//        return cleanUserId(super.getString("HCP"));
//    }
//    public String getReferralUserType() {
//        return super.getString("HCP_TYPE");
//    }
//    public String getReferralRecipientUserId() {
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
