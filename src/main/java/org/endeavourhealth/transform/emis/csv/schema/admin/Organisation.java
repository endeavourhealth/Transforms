package org.endeavourhealth.transform.emis.csv.schema.admin;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;

import java.util.UUID;

public class Organisation extends AbstractCsvParser {

    public Organisation(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath, boolean openParser) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, openParser, EmisCsvToFhirTransformer.CSV_FORMAT, EmisCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD, EmisCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "OrganisationGuid",
                "CDB",
                "OrganisationName",
                "ODSCode",
                "ParentOrganisationGuid",
                "CCGOrganisationGuid",
                "OrganisationType",
                "OpenDate",
                "CloseDate",
                "MainLocationGuid",
                "ProcessingId"
        };
    }

    @Override
    protected String getFileTypeDescription() {
        return "Emis organisations file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getOrganisationGuid() {
        return super.getCell("OrganisationGuid");
    }
    public CsvCell getCDB() {
        return super.getCell("CDB");
    }
    public CsvCell getOrganisatioName() {
        return super.getCell("OrganisationName");
    }
    public CsvCell getODScode() {
        return super.getCell("ODSCode");
    }
    public CsvCell getParentOrganisationGuid() {
        return super.getCell("ParentOrganisationGuid");
    }
    public CsvCell getCCGOrganisationGuid() {
        return super.getCell("CCGOrganisationGuid");
    }
    public CsvCell getOrganisationType() {
        return super.getCell("OrganisationType");
    }
    public CsvCell getOpenDate() {
        return super.getCell("OpenDate");
    }
    public CsvCell getCloseDate() {
        return super.getCell("CloseDate");
    }
    public CsvCell getMainLocationGuid() {
        return super.getCell("MainLocationGuid");
    }
    public CsvCell getProcessingId() {
        return super.getCell("ProcessingId");
    }

    /*public String getOrganisationGuid() {
        return super.getString("OrganisationGuid");
    }
    public Integer getCDB() {
        return super.getInt("CDB");
    }
    public String getOrganisatioName() {
        return super.getString("OrganisationName");
    }
    public String getODScode() {
        return super.getString("ODSCode");
    }
    public String getParentOrganisationGuid() {
        return super.getString("ParentOrganisationGuid");
    }
    public String getCCGOrganisationGuid() {
        return super.getString("CCGOrganisationGuid");
    }
    public String getOrganisationType() {
        return super.getString("OrganisationType");
    }
    public Date getOpenDate() throws TransformException {
        return super.getDate("OpenDate");
    }
    public Date getCloseDate() throws TransformException {
        return super.getDate("CloseDate");
    }
    public String getMainLocationGuid() {
        return super.getString("MainLocationGuid");
    }
    public Integer getProcessingId() {
        return super.getInt("ProcessingId");
    }*/
}
