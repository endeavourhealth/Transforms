package org.endeavourhealth.transform.tpp.csv.schema.admin;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SROrganisation extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SROrganisation.class);

    public SROrganisation(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                TppCsvToFhirTransformer.CSV_FORMAT,
                TppCsvToFhirTransformer.DATE_FORMAT,
                TppCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        if (version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK)) {
            return new String[]{
                    "RowIdentifier",
                    "Name",
                    "ID",
                    "HouseName",
                    "HouseNumber",
                    "NameOfRoad",
                    "NameOfLocality",
                    "NameOfTown",
                    "NameOfCounty",
                    "FullPostCode",
                    "Telephone",
                    "SecondaryTelephone",
                    "Fax",
                    "MadeObsolete",
                    "IDTrust",
                    "IDCcg"
            };
        } else {
            return new String[]{
                    "RowIdentifier",
                    "Name",
                    "ID",
                    "HouseName",
                    "HouseNumber",
                    "NameOfRoad",
                    "NameOfLocality",
                    "NameOfTown",
                    "NameOfCounty",
                    "FullPostCode",
                    "Telephone",
                    "SecondaryTelephone",
                    "Fax",
                    "MadeObsolete",
                    "IDTrust",
                    "IDCcg",
                    "RemovedData"
            };
        }
    }


    public CsvCell getRowIdentifier() {
        return super.getCell("RowIdentifier");
    }

    public CsvCell getName() {
        return super.getCell("Name");
    }

    public CsvCell getID() {
        return super.getCell("ID");
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

    public CsvCell getFullPostCode() {
        return super.getCell("FullPostCode");
    }

    public CsvCell getTelephone() {
        return super.getCell("Telephone");
    }

    public CsvCell getSecondaryTelephone() {
        return super.getCell("SecondaryTelephone");
    }

    public CsvCell getFax() {
        return super.getCell("Fax");
    }

    public CsvCell getMadeObsolete() {
        return super.getCell("MadeObsolete");
    }

    public CsvCell getIDTrust() {
        return super.getCell("IDTrust");
    }

    public CsvCell getIDCcg() {
        return super.getCell("IDCcg");
    }

    public CsvCell getRemovedData() {
        return super.getCell("RemovedData");
    }

    ;

    @Override
    protected String getFileTypeDescription() {
        return "TPP Organisation Entry file ";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

}
