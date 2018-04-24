package org.endeavourhealth.transform.tpp.csv.schema.admin;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRTrust extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRTrust.class);

    public SRTrust(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                TppCsvToFhirTransformer.CSV_FORMAT,
                TppCsvToFhirTransformer.DATE_FORMAT,
                TppCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        //TODO - update transform to check for null cells when using fields not in the older version
        if (version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK)
                || version.equals(TppCsvToFhirTransformer.VERSION_87)) {
            return new String[]{
                    "RowIdentifier",
                    "DateCreated",
                    "IdProfileCreatedBy",
                    "Name",
                    "OdsCode",
                    "HouseName",
                    "HouseNumber",
                    "NameOfRoad",
                    "NameOfLocality",
                    "NameOfTown",
                    "NameOfCounty",
                    "FullPostCode",
                    "Telephone",
                    "SecondaryTelephone",
                    "Fax"
            };
        } else {
            return new String[]{
                    "RowIdentifier",
                    "DateCreated",
                    "IdProfileCreatedBy",
                    "Name",
                    "OdsCode",
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
                    "RemovedData"
            };
        }
    }

    public CsvCell getRowIdentifier() {
        return super.getCell("RowIdentifier");
    }

    public CsvCell getDateCreated() {
        return super.getCell("DateCreated");
    }

    public CsvCell getIdProfileCreatedBy() {
        return super.getCell("IdProfileCreatedBy");
    }

    public CsvCell getName() {
        return super.getCell("Name");
    }

    public CsvCell getOdsCode() {
        return super.getCell("OdsCode");
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

    public CsvCell getRemovedData() {
        return super.getCell("RemovedData");
    }


    //TODO fix the string below to make it meaningful
    @Override
    protected String getFileTypeDescription() {
        return "TPP SRTrust Entry file ";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
