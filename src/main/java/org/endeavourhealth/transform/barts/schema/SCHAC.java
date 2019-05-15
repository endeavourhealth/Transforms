package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SCHAC extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(SCHAC.class);

    public SCHAC(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                null, null); //all Barts date parsing for Power Insight content should use BartsCsvHelper.parseDate(..)
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[] {
                "#SCHEDULE_ACTION_ID",
                "EXTRACT_DT_TM",
                "ACTIVE_IND",
                "SCHEDULE_ID",
                "SCH_EVENT_ID",
                "ENCNTR_ID",
                "ACTION_DESCRIPTION_CD",
                "ACTION_DT_TM",
                "ACTION_PRSNL_ID",
                "ACTION_REASON_CD",
                "PERFORMED_DT_TM"
        };
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
