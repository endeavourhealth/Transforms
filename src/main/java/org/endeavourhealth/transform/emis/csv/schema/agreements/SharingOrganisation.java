package org.endeavourhealth.transform.emis.csv.schema.agreements;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;

import java.util.UUID;

public class SharingOrganisation extends AbstractCsvParser {

    public SharingOrganisation(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath, boolean openParser) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, openParser, EmisCsvToFhirTransformer.CSV_FORMAT, EmisCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD, EmisCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "OrganisationGuid",
                "IsActivated",
                "LastModifiedDate",
                "Disabled",
                "Deleted"
        };
    }

    @Override
    protected String getFileTypeDescription() {
        return "Emis sharing agreements file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getOrganisationGuid() {
        return getCell("OrganisationGuid");
    }
    public CsvCell getIsActivated() {
        return getCell("IsActivated");
    }
    public CsvCell getLastModifiedDate() {
        return getCell("LastModifiedDate");
    }
    public CsvCell getDisabled() {
        return getCell("Disabled");
    }
    public CsvCell getDeleted() {
        return getCell("Deleted");
    }

    /*public String getOrganisationGuid() {
        return getString("OrganisationGuid");
    }
    public boolean getIsActivated() {
        return getBoolean("IsActivated");
    }
    public Date getLastModifiedDate() throws Exception {
        return getDate("LastModifiedDate");
    }
    public boolean getDisabled() {
        return getBoolean("Disabled");
    }
    public boolean getDeleted() {
        return getBoolean("Deleted");
    }*/
}
