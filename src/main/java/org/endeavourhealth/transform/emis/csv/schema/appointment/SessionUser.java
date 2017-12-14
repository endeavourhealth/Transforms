package org.endeavourhealth.transform.emis.csv.schema.appointment;

import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;
import org.endeavourhealth.transform.emis.csv.schema.AbstractCsvParser;

public class SessionUser extends AbstractCsvParser {

    public SessionUser(String version, String filePath, boolean openParser) throws Exception {
        super(version, filePath, openParser, EmisCsvToFhirTransformer.CSV_FORMAT, EmisCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD, EmisCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "SessionGuid",
                "UserInRoleGuid",
                "Deleted",
                "ProcessingId"
        };
    }

    public String getSessionGuid() {
        return super.getString("SessionGuid");
    }
    public String getUserInRoleGuid() {
        return super.getString("UserInRoleGuid");
    }
    public boolean getdDeleted() {
        return super.getBoolean("Deleted");
    }
    public int getProcessingId() {
        return super.getInt("ProcessingId");
    }
}
