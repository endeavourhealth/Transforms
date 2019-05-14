package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class RTTPE extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(RTTPE.class);

    public RTTPE(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                null, null); //all Barts date parsing for Power Insight content should use BartsCsvHelper.parseDate(..)
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[] {
                "#PERIOD_KEY",
                "ACTIVE_IND",
                "EXTRACT_DT_TM",
                "EPISODE_ID",
                "PAT_PATHWAY_IDENT",
                "PERIOD_SEQ_NBR",
                "PERIOD_START_DT",
                "PERIOD_END_DT",
                "PERIOD_START_EVENT_IDENT",
                "PERIOD_END_EVENT_IDENT"
        };
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
