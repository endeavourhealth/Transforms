package org.endeavourhealth.transform.tpp.csv.schema.admin;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SROrganisationBranch extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SROrganisationBranch.class);

    public SROrganisationBranch(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                    "IDOrganisationVisibleTo",
                    "ID",
                    "BranchName",
                    "HouseName",
                    "HouseNumber",
                    "RoadName",
                    "Locality",
                    "Town",
                    "County",
                    "PostCode",
                    "BranchObsolete",
                    "IDOrganisation"
            };
        } else {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "ID",
                    "BranchName",
                    "HouseName",
                    "HouseNumber",
                    "RoadName",
                    "Locality",
                    "Town",
                    "County",
                    "PostCode",
                    "BranchObsolete",
                    "IDOrganisation",
                    "RemovedData"
            };
        }
    }


    public CsvCell getRowIdentifier() {
        return super.getCell("RowIdentifier");
    }

    public CsvCell getIDOrganisationVisibleTo() {
        return super.getCell("IDOrganisationVisibleTo");
    }

    public CsvCell getID() {
        return super.getCell("ID");
    }

    public CsvCell getBranchName() {
        return super.getCell("BranchName");
    }

    public CsvCell getHouseName() {
        return super.getCell("HouseName");
    }

    public CsvCell getHouseNumber() {
        return super.getCell("HouseNumber");
    }

    public CsvCell getRoadName() {
        return super.getCell("RoadName");
    }

    public CsvCell getLocality() {
        return super.getCell("Locality");
    }

    public CsvCell getTown() {
        return super.getCell("Town");
    }

    public CsvCell getCounty() {
        return super.getCell("County");
    }

    public CsvCell getPostCode() {
        return super.getCell("PostCode");
    }

    public CsvCell getBranchObsolete() {
        return super.getCell("BranchObsolete");
    }

    public CsvCell getIDOrganisation() {
        return super.getCell("IDOrganisation");
    }

    public CsvCell getRemovedData() {
        return super.getCell("RemovedData");
    }


    @Override
    protected String getFileTypeDescription() {
        return "TPP Organisation Branch Entry file ";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

}
