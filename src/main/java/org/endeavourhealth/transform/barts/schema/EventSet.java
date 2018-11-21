package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class EventSet extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(EventSet.class);

    public EventSet(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT.withHeader(getCsvHeadersForVersion(version)), //file doesn't contain headers
                BartsCsvToFhirTransformer.DATE_FORMAT,
                BartsCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return getCsvHeadersForVersion(version);
    }

    /**
     * unlike the other files, we don't get column headers in this file, so need
     * to be able to PASS IN the headers into the parser, so we've got this static method to allow that
     */
    private static String[] getCsvHeadersForVersion(String version) {
        return new String[] {
                "ACCUMULATION_IND",
                "CATEGORY_FLAG",
                "EVENT_SET_CD_DEFINITION",
                "EVENT_SET_CD_DESCR",
                "EVENT_SET_CD_DISP",
                "EVENT_SET_CD_DISP_KEY",
                "CODE_STATUS_CD",
                "EVENT_SET_CD",
                "COMBINE_FORMAT",
                "EVENT_SET_COLOR_NAME",
                "EVENT_SET_ICON_NAME",
                "EVENT_SET_NAME",
                "EVENT_SET_NAME_KEY",
                "EVENT_SET_STATUS_CD",
                "GROUPING_RULE_FLAG",
                "LEAF_EVENT_CD_COUNT",
                "OPERATION_DISPLAY_FLAG",
                "OPERATION_FORMULA",
                "PRIMITIVE_EVENT_SET_COUNT",
                "SHOW_IF_NO_DATA_IND",
                "UPDT_APPLCTX",
                "UPDT_CNT",
                "UPDT_DT_TM",
                "UPDT_ID",
                "UPDT_TASK",
                "DISPLAY_ASSOCIATION_IND"
        };
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
