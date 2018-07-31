package org.endeavourhealth.transform.tpp.csv.schema.codes;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRMapping extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRMapping.class);

    public SRMapping(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                TppCsvToFhirTransformer.CSV_FORMAT,
                TppCsvToFhirTransformer.DATE_FORMAT,
                TppCsvToFhirTransformer.TIME_FORMAT);

    }

    @Override
    protected String[] getCsvHeaders(String version) {
        if (version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK_2)) {
            return new String[]{
                    "RowIdentifier",
                    "IdMappingGroup",
            };
        } else {
            return new String[]{
                    "RowIdentifier",
                    "IdMappingGroup",
                    "Mapping"
            };
        }
    }

    public CsvCell getRowIdentifier() {
        return super.getCell("RowIdentifier");
    }

    public CsvCell getIDMappingGroup() {
        return super.getCell("IdMappingGroup");
    }

    public CsvCell getMapping() {
        return super.getCell("Mapping");
    }

    @Override
    protected String getFileTypeDescription() {
        return "TPP Mapping file providing textual descriptions of static list items referenced within the other extract files ";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
