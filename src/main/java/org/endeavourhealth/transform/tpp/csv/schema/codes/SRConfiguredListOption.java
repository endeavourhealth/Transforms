package org.endeavourhealth.transform.tpp.csv.schema.codes;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRConfiguredListOption extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRConfiguredListOption.class);

    public SRConfiguredListOption(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                TppCsvToFhirTransformer.CSV_FORMAT,
                TppCsvToFhirTransformer.DATE_FORMAT,
                TppCsvToFhirTransformer.TIME_FORMAT,
                TppCsvToFhirTransformer.ENCODING);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        if (version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK_3)) {
            return new String[]{
                    "RowIdentifier",
                    "ConfiguredList",
                    "ConfiguredListOption",
                    "CDSCode",
                    "MHLDDSCode",
                    "CAMHSCode",
                    "MHSDSCode",
                    "RemovedData"
            };
        } else if (version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK)
                || version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK_2)
                ) {
            return new String[]{
                    "RowIdentifier",
                    "ConfiguredList",
                    "ConfiguredListOption",
                    "CDSCode",
                    "MHLDDSCode",
                    "CAMHSCode",
                    "MHSDSCode"
            };
        } else if (version.equals(TppCsvToFhirTransformer.VERSION_87)
                || version.equals(TppCsvToFhirTransformer.VERSION_88)) {
            return new String[]{
                    "RowIdentifier",
                    "ConfiguredList",
                    "ConfiguredListOption",
                    "CDSCode",
                    "CAMHSCode",
                    "MHSDSCode"
            };
        } else if (version.equals(TppCsvToFhirTransformer.VERSION_89)
                || version.equals(TppCsvToFhirTransformer.VERSION_90)) {
            return new String[]{
                    "RowIdentifier",
                    "ConfiguredList",
                    "ConfiguredListOption",
                    "CDSCode",
                    "CAMHSCode",
                    "MHSDSCode",
                    "RemovedData"
            };
        } else {
            return new String[]{
                    "RowIdentifier",
                    "ConfiguredList",
                    "ConfiguredListOption",
                    "CDSCode",
                    "CAMHSCode",
                    "MHSDSCode"
            };
        }
    }

    public CsvCell getRowIdentifier() {
        return super.getCell("RowIdentifier");
    }

    public CsvCell getConfiguredList() {
        return super.getCell("ConfiguredList");
    }

    public CsvCell getConfiguredListOption() {
        return super.getCell("ConfiguredListOption");
    }

    public CsvCell getCDSCode() {
        return super.getCell("CDSCode");
    }

    public CsvCell getMHLDDSCode() {
        return super.getCell("MHLDDSCode");
    }

    public CsvCell getCAMHSCode() {
        return super.getCell("CAMHSCode");
    }

    public CsvCell getMHSDSCode() {
        return super.getCell("MHSDSCode");
    }

    public CsvCell getRemovedData() {
        return super.getCell("RemovedData");
    }


    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
