package org.endeavourhealth.transform.emis.csv.schema.admin;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;

import java.util.UUID;

public class OrganisationLocation extends AbstractCsvParser {

    public OrganisationLocation(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath, boolean openParser) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, openParser, EmisCsvToFhirTransformer.CSV_FORMAT, EmisCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD, EmisCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "OrganisationGuid",
                "LocationGuid",
                "IsMainLocation",
                "Deleted",
                "ProcessingId"
        };
    }

    @Override
    protected String getFileTypeDescription() {
        return "Emis organisation-location link file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getOrgansationGuid() {
        return super.getCell("OrganisationGuid");
    }
    public CsvCell getLocationGuid() {
        return super.getCell("LocationGuid");
    }
    public CsvCell getIsMainLocation() {
        return super.getCell("IsMainLocation");
    }
    public CsvCell getDeleted() {
        return super.getCell("Deleted");
    }
    public CsvCell getProcessingId() {
        return super.getCell("ProcessingId");
    }

    /*public String getOrgansationGuid() {
        return super.getString("OrganisationGuid");
    }
    public String getLocationGuid() {
        return super.getString("LocationGuid");
    }
    public boolean getIsMainLocation() {
        return super.getBoolean("IsMainLocation");
    }
    public int getIsMainLocationAsInt() {
        return super.getInt("IsMainLocation");
    }
    public boolean getDeleted() {
        return super.getBoolean("Deleted");
    }
    public Integer getProcessingId() {
        return super.getInt("ProcessingId");
    }*/
}
