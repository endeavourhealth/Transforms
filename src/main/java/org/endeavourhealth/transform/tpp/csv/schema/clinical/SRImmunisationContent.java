package org.endeavourhealth.transform.tpp.csv.schema.clinical;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRImmunisationContent extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRImmunisationContent.class);

    public SRImmunisationContent(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                TppCsvToFhirTransformer.CSV_FORMAT,
                TppCsvToFhirTransformer.DATE_FORMAT,
                TppCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        //TODO - update transform to check for null cells when using fields not in the older version
        if (version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK)
                || version.equals(TppCsvToFhirTransformer.VERSION_88)) {
            return new String[]{
                    "RowIdentifier",
                    "Name",
                    "Content",
                    "DateDeleted"
            };
        } else if (version.equals(TppCsvToFhirTransformer.VERSION_87)
                || version.equals(TppCsvToFhirTransformer.VERSION_89)) {
            return new String[]{
                    "RowIdentifier",
                    "Name",
                    "Content",
                    "DateDeleted",
                    "RemovedData"
            };
        } else {
            return new String[]{
                    "RowIdentifier",
                    "Name",
                    "Content",
                    "DateDeleted"
            };
        }
    }

    public CsvCell getRowIdentifier() {
        return super.getCell("RowIdentifier");
    }

    public CsvCell getName() {
        return super.getCell("Name");
    }

    public CsvCell getContent() {
        return super.getCell("Content");
    }

    public CsvCell getDateDeleted() {
        return super.getCell("DateDeleted");
    }

    public CsvCell getRemovedData() {
        return super.getCell("RemovedData");
    }


    //TODO fix the string below to make it meaningful
    @Override
    protected String getFileTypeDescription() {
        return "TPP Immunisation Content Entry file ";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
