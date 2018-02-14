package org.endeavourhealth.transform.emis.csv.schema.admin;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;

import java.util.UUID;

public class Location extends AbstractCsvParser {

    public Location(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath, boolean openParser) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, openParser, EmisCsvToFhirTransformer.CSV_FORMAT, EmisCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD, EmisCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "LocationGuid",
                "LocationName",
                "LocationTypeDescription",
                "ParentLocationGuid",
                "OpenDate",
                "CloseDate",
                "MainContactName",
                "FaxNumber",
                "EmailAddress",
                "PhoneNumber",
                "HouseNameFlatNumber",
                "NumberAndStreet",
                "Village",
                "Town",
                "County",
                "Postcode",
                "Deleted",
                "ProcessingId"
        };
    }

    @Override
    protected String getFileTypeDescription() {
        return "Emis organisation location file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getLocationGuid() {
        return super.getCell("LocationGuid");
    }
    public CsvCell getLocationName() {
        return super.getCell("LocationName");
    }
    public CsvCell getLocationTypeDescription() {
        return super.getCell("LocationTypeDescription");
    }
    public CsvCell getParentLocationId() {
        return super.getCell("ParentLocationGuid");
    }
    public CsvCell getOpenDate() {
        return super.getCell("OpenDate");
    }
    public CsvCell getCloseDate() {
        return super.getCell("CloseDate");
    }
    public CsvCell getMainContactName() {
        return super.getCell("MainContactName");
    }
    public CsvCell getFaxNumber() {
        return super.getCell("FaxNumber");
    }
    public CsvCell getEmailAddress() {
        return super.getCell("EmailAddress");
    }
    public CsvCell getPhoneNumber() {
        return super.getCell("PhoneNumber");
    }
    public CsvCell getHouseNameFlatNumber() {
        return super.getCell("HouseNameFlatNumber");
    }
    public CsvCell getNumberAndStreet() {
        return super.getCell("NumberAndStreet");
    }
    public CsvCell getVillage() {
        return super.getCell("Village");
    }
    public CsvCell getTown() {
        return super.getCell("Town");
    }
    public CsvCell getCounty() {
        return super.getCell("County");
    }
    public CsvCell getPostcode() {
        return super.getCell("Postcode");
    }
    public CsvCell getDeleted() {
        return super.getCell("Deleted");
    }
    public CsvCell getProcessingId() {
        return super.getCell("ProcessingId");
    }

    /*public String getLocationGuid() {
        return super.getString("LocationGuid");
    }
    public String getLocationName() {
        return super.getString("LocationName");
    }
    public String getLocationTypeDescription() {
        return super.getString("LocationTypeDescription");
    }
    public String getParentLocationId() {
        return super.getString("ParentLocationGuid");
    }
    public Date getOpenDate() throws TransformException {
        return super.getDate("OpenDate");
    }
    public Date getCloseDate() throws TransformException {
        return super.getDate("CloseDate");
    }
    public String getMainContactName() {
        return super.getString("MainContactName");
    }
    public String getFaxNumber() {
        return super.getString("FaxNumber");
    }
    public String getEmailAddress() {
        return super.getString("EmailAddress");
    }
    public String getPhoneNumber() {
        return super.getString("PhoneNumber");
    }
    public String getHouseNameFlatNumber() {
        return super.getString("HouseNameFlatNumber");
    }
    public String getNumberAndStreet() {
        return super.getString("NumberAndStreet");
    }
    public String getVillage() {
        return super.getString("Village");
    }
    public String getTown() {
        return super.getString("Town");
    }
    public String getCounty() {
        return super.getString("County");
    }
    public String getPostcode() {
        return super.getString("Postcode");
    }
    public boolean getDeleted() {
        return super.getBoolean("Deleted");
    }
    public Integer getProcessingId() {
        return super.getInt("ProcessingId");
    }*/





}