package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class EventSetCanon extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(EventSetCanon.class);

    public EventSetCanon(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                "PARENT_EVENT_SET_CD",
                "EVENT_SET_CD",
                "EVENT_SET_COLLATING_SEQ",
                "EVENT_SET_EXPLODE_IND",
                "EVENT_SET_STATUS_CD",
                "UPDT_DT_TM",
                "UPDT_TASK",
                "UPDT_ID",
                "UPDT_CNT",
                "UPDT_APPLCTX",
                "LAST_UTC_TS",
                "INST_ID"
        };
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
