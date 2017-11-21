package org.endeavourhealth.transform.vision.schema;

import org.endeavourhealth.transform.common.exceptions.TransformException;
import org.endeavourhealth.transform.emis.csv.schema.AbstractCsvParser;
import org.endeavourhealth.transform.vision.VisionCsvToFhirTransformer;

import java.io.File;
import java.util.Date;

public class Referral extends AbstractCsvParser {

    public Referral(String version, File f, boolean openParser) throws Exception {
        super(version, f, openParser, VisionCsvToFhirTransformer.CSV_FORMAT, VisionCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD, VisionCsvToFhirTransformer.TIME_FORMAT);
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
        return super.getString("HCP");
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
