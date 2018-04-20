package org.endeavourhealth.transform.tpp.csv.schema.unused;

import org.endeavourhealth.transform.barts.schema.PPALI;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRAAddressBookEntry extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(PPALI.class);

    public SRAAddressBookEntry(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                TppCsvToFhirTransformer.CSV_FORMAT,
                TppCsvToFhirTransformer.DATE_FORMAT,
                TppCsvToFhirTransformer.TIME_FORMAT);
    }


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
    public CsvCell getRowId() { return super.getCell("RowIdentifier"); };
    public CsvCell  getIdOrgVisibleTo() {return super.getCell("IDOrganisationVisibleTo");}
    public CsvCell  getDateCreated() {return super.getCell("DateCreated");}
    public CsvCell  getTitle() {return super.getCell("Title");}
    public CsvCell  getFirstName() {return super.getCell("FirstName");}
    public CsvCell  getMiddleNames() {return super.getCell("MiddleNames");}
    public CsvCell  getSurname() {return super.getCell("Surname");}
    public CsvCell  getOrgName() {return super.getCell("OrganisationName");}
    public CsvCell  getIdParentAddressBookEntry() {return super.getCell("IDParentAddressBookEntry");}
    public CsvCell  getHouseName() {return super.getCell("HouseName");}
    public CsvCell  getHouseNbr() {return super.getCell("HouseNumber");}
    public CsvCell  getNameOfRoad() {return super.getCell("NameOfRoad");}
    public CsvCell  getNameofLocality() {return super.getCell("NameOfLocality");}
    public CsvCell  getTown() {return super.getCell("NameOfTown");}
    public CsvCell  getCounty() {return super.getCell("NameOfCounty");}
    public CsvCell  getPostCode() {return super.getCell("FullPostcode");}
    @Override
    protected String getFileTypeDescription() {
        return "TPP SRA Address Book Entry file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
