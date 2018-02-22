package org.endeavourhealth.transform.emis.csv.schema.appointment;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;

import java.util.UUID;

public class SessionUser extends AbstractCsvParser {

    public SessionUser(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, EmisCsvToFhirTransformer.CSV_FORMAT, EmisCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD, EmisCsvToFhirTransformer.TIME_FORMAT);
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

    @Override
    protected String getFileTypeDescription() {
        return "Emis session-user link file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getSessionGuid() {
        return super.getCell("SessionGuid");
    }
    public CsvCell getUserInRoleGuid() {
        return super.getCell("UserInRoleGuid");
    }
    public CsvCell getdDeleted() {
        return super.getCell("Deleted");
    }
    public CsvCell getProcessingId() {
        return super.getCell("ProcessingId");
    }

    /*public String getSessionGuid() {
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
    }*/
}
