package org.endeavourhealth.transform.emis.csv.schema.audit;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;

import java.util.Date;
import java.util.UUID;

public class RegistrationAudit extends AbstractCsvParser {

    public RegistrationAudit(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath, boolean openParser) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, openParser, EmisCsvToFhirTransformer.CSV_FORMAT, EmisCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD, EmisCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "PatientGuid",
                "OrganisationGuid",
                "ModifiedDate",
                "ModifiedTime",
                "UserInRoleGuid",
                "ModeType",
                "ProcessingId"
        };
    }

    @Override
    protected String getFileTypeDescription() {
        return "Emis registration audit trail file";
    }

    @Override
    protected boolean isFileAudited() {
        return false;
    }

    public String getPatientGuid() {
        return getString("PatientGuid");
    }

    public String getOrganisationGuid() {
        return getString("OrganisationGuid");
    }

    public Date getModifiedDateTime() throws Exception {
        return getDateTime("ModifiedDate", "ModifiedTime");
    }

    public String getUserInRoleGuid() {
        return getString("UserInRoleGuid");
    }

    public String getModeType() {
        return getString("ModeType");
    }

    public Integer getProcessingId() {
        return super.getInt("ProcessingId");
    }


}
