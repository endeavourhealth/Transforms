package org.endeavourhealth.transform.vision.schema;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.emis.csv.schema.AbstractCsvParser;
import org.endeavourhealth.transform.vision.VisionCsvToFhirTransformer;

import java.util.Date;

import static org.endeavourhealth.transform.vision.VisionCsvToFhirTransformer.cleanUserId;

public class Referral extends AbstractCsvParser {

    public Referral(String version, String filePath, boolean openParser) throws Exception {
        super(version, filePath, openParser, VisionCsvToFhirTransformer.CSV_FORMAT.withHeader(
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

    public String getPatientID() {
        return super.getString("PID");
    }
    public String getReferralID() {
        return super.getString("ID");
    }
    public Date getReferralDate() throws TransformException {
        return super.getDate("DATE");
    }
    public Date getActionDate() throws TransformException {
        return super.getDate("ACTION_DATE");
    }
    public String getReferralUserID() {
        return cleanUserId(super.getString("HCP"));
    }
    public String getReferralUserType() {
        return super.getString("HCP_TYPE");
    }
    public String getReferralDestOrgID() {
        return super.getString("TO_HCP");
    }
    public String getOrganisationID() {
        return super.getString("SERVICE_ID");
    }
    public String getAction() {
        return super.getString("ACTION");
    }
    public String getReferralType() {
        return super.getString("TYPE");
    }
    public String getLinks() {
        return super.getString("LINKS");
    }

}
