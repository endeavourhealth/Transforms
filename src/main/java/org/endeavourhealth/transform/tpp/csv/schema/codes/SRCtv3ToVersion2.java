package org.endeavourhealth.transform.tpp.csv.schema.codes;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRCtv3ToVersion2 extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRCtv3ToVersion2.class);

    public SRCtv3ToVersion2(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                TppCsvToFhirTransformer.CSV_FORMAT,
                TppCsvToFhirTransformer.DATE_FORMAT,
                TppCsvToFhirTransformer.TIME_FORMAT,
                TppCsvToFhirTransformer.ENCODING);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        if (version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK)
                || version.equals(TppCsvToFhirTransformer.VERSION_87)
                || version.equals(TppCsvToFhirTransformer.VERSION_88)
                || version.equals(TppCsvToFhirTransformer.VERSION_91)
                || version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK_2)) {
            return new String[]{
                    "RowIdentifier",
                    "Ctv3Code",
                    "Version2Code"
            };
        } else if (version.equals(TppCsvToFhirTransformer.VERSION_89)
                || version.equals(TppCsvToFhirTransformer.VERSION_90)
                || version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK_3)) {
            return new String[]{
                    "RowIdentifier",
                    "Ctv3Code",
                    "Version2Code",
                    "RemovedData"
            };

        } else {
            return new String[]{
                    "RowIdentifier",
                    "Ctv3Code",
                    "Version2Code"
            };
        }
    }

    public CsvCell getRowIdentifier() {
        return super.getCell("RowIdentifier");
    }

    public CsvCell getCtv3Code() {
        return super.getCell("Ctv3Code");
    }

    public CsvCell getVersion2Code() {
        return super.getCell("Version2Code");
    }

    public CsvCell getRemovedData() {
        return super.getCell("RemovedData");
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
