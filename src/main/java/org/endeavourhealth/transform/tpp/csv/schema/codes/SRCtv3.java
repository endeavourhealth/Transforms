package org.endeavourhealth.transform.tpp.csv.schema.codes;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRCtv3 extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRCtv3.class);

    public SRCtv3(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                TppCsvToFhirTransformer.CSV_FORMAT,
                TppCsvToFhirTransformer.DATE_FORMAT,
                TppCsvToFhirTransformer.TIME_FORMAT,
                TppCsvToFhirTransformer.ENCODING);
    }

    @Override
    protected String[] getCsvHeaders(String version) {

        if (version.equals(TppCsvToFhirTransformer.VERSION_87)
                || version.equals(TppCsvToFhirTransformer.VERSION_89)
                || version.equals(TppCsvToFhirTransformer.VERSION_90)
                || version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK_3)
                || version.equals(TppCsvToFhirTransformer.VERSION_93)) {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "Ctv3Code",
                    "Ctv3Text",
                    "RemovedData"
            };

        } else {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "Ctv3Code",
                    "Ctv3Text"
            };
        }
    }

    public CsvCell getRowIdentifier() {
        return super.getCell("RowIdentifier");
    }

    public CsvCell getIDOrganisationVisibleTo() {
        return super.getCell("IDOrganisationVisibleTo");
    }

    public CsvCell getCtv3Code() {
        return super.getCell("Ctv3Code");
    }

    public CsvCell getCtv3Text() {
        return super.getCell("Ctv3Text");
    }

    public CsvCell getRemovedData() {
        return super.getCell("RemovedData");
    }


    @Override
    protected boolean isFileAudited() {
        //this file is just used to populate a lookup table, so don't audit
        return false;
    }
}
