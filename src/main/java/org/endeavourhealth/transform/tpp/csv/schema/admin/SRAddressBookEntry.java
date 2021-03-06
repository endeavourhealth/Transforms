package org.endeavourhealth.transform.tpp.csv.schema.admin;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRAddressBookEntry extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRAddressBookEntry.class);

    public SRAddressBookEntry(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                TppCsvToFhirTransformer.CSV_FORMAT,
                TppCsvToFhirTransformer.DATE_FORMAT,
                TppCsvToFhirTransformer.TIME_FORMAT,
                TppCsvToFhirTransformer.ENCODING);
    }
    // NB So far this is the same across versions
    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "RowIdentifier",
                "IDOrganisationVisibleTo",
                "DateCreated",
                "Title",
                "FirstName",
                "MiddleNames",
                "Surname",
                "OrganisationName",
                "IDParentAddressBookEntry",
                "HouseName",
                "HouseNumber",
                "NameOfRoad",
                "NameOfLocality",
                "NameOfTown",
                "NameOfCounty",
                "FullPostcode"
        };
    }

    public CsvCell getRowIdentifier() {
        return super.getCell("RowIdentifier");
    }

    public CsvCell getIDOrganisationVisibleTo() {
        return super.getCell("IDOrganisationVisibleTo");
    }

    public CsvCell getDateCreated() {
        return super.getCell("DateCreated");
    }

    public CsvCell getTitle() {
        return super.getCell("Title");
    }

    public CsvCell getFirstName() {
        return super.getCell("FirstName");
    }

    public CsvCell getMiddleNames() {
        return super.getCell("MiddleNames");
    }

    public CsvCell getSurname() {
        return super.getCell("Surname");
    }

    public CsvCell getOrganisationName() {
        return super.getCell("OrganisationName");
    }

    public CsvCell getIDParentAddressBookEntry() {
        return super.getCell("IDParentAddressBookEntry");
    }

    public CsvCell getHouseName() {
        return super.getCell("HouseName");
    }

    public CsvCell getHouseNumber() {
        return super.getCell("HouseNumber");
    }

    public CsvCell getNameOfRoad() {
        return super.getCell("NameOfRoad");
    }

    public CsvCell getNameOfLocality() {
        return super.getCell("NameOfLocality");
    }

    public CsvCell getNameOfTown() {
        return super.getCell("NameOfTown");
    }

    public CsvCell getNameOfCounty() {
        return super.getCell("NameOfCounty");
    }

    public CsvCell getFullPostcode() {
        return super.getCell("FullPostcode");
    }


    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
