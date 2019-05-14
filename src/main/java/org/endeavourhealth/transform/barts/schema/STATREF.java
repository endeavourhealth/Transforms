package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class STATREF extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(STATREF.class);

    public STATREF(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                null, null); //all Barts date parsing for Power Insight content should use BartsCsvHelper.parseDate(..)
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[] {
                "#HEALTH_SYSTEM_SOURCE_ID",
                "SOURCE_COUNT",
                "EXTRACT_FILENAME",
                "EXTRACT_RANGE_START_DT",
                "EXTRACT_RANGE_END_DT",
                "EXTRACT_START_DT_TM",
                "EXTRACT_END_DT_TM"
        };
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
