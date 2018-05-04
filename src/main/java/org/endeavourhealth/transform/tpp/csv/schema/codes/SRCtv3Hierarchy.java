package org.endeavourhealth.transform.tpp.csv.schema.codes;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRCtv3Hierarchy extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRCtv3Hierarchy.class);

    public SRCtv3Hierarchy(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                TppCsvToFhirTransformer.CSV_FORMAT,
                TppCsvToFhirTransformer.DATE_FORMAT,
                TppCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        //TODO - update transform to check for null cells when using fields not in the older version
        if (version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK)) {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "Ctv3CodeParent",
                    "Ctv3CodeChild",
                    "ChildLevel"
            };
        } else {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "Ctv3CodeParent",
                    "Ctv3CodeChild",
                    "ChildLevel",
                    "RemovedData"
            };
        }
    }

    // RowIdentifier, IDOrganisationVisibleTo, Ctv3CodeParent, Ctv3CodeChild, ChildLevel, RemovedData

    public CsvCell getRowIdentifier() {
        return super.getCell("RowIdentifier");
    }

    public CsvCell getIDOrganisationVisibleTo() {
        return super.getCell("IDOrganisationVisibleTo");
    }

    public CsvCell getCtv3CodeParent() {
        return super.getCell("Ctv3CodeParent");
    }

    public CsvCell getCtv3CodeChild() {
        return super.getCell("Ctv3CodeChild");
    }

    public CsvCell getChildLevel() {
        return super.getCell("ChildLevel");
    }

    public CsvCell getRemovedData() {
        return super.getCell("RemovedData");
    }

    @Override
    protected String getFileTypeDescription() {
        return "TPP CTV3 Hierarchy file";
    }

    @Override
    protected boolean isFileAudited() {
        //data from this file is used to populate a reference table, but it's not actually used
        //to look up content, so no need to audit it
        return false;
    }
}
