package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class EventCode extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(EventCode.class);

    public EventCode(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                "EVENT_CD",
                "EVENT_CD_DEFINITION",
                "EVENT_CD_DESCR",
                "EVENT_CD_DISP",
                "EVENT_CD_DISP_KEY",
                "CODE_STATUS_CD",
                "DEF_DOCMNT_ATTRIBUTES",
                "DEF_DOCMNT_FORMAT_CD",
                "DEF_DOCMNT_STORAGE_CD",
                "DEF_EVENT_CLASS_CD",
                "DEF_EVENT_CONFID_LEVEL_CD",
                "DEF_EVENT_LEVEL",
                "EVENT_ADD_ACCESS_IND",
                "EVENT_CD_SUBCLASS_CD",
                "EVENT_CHG_ACCESS_IND",
                "EVENT_SET_NAME",
                "RETENTION_DAYS",
                "UPDT_APPLCTX",
                "UPDT_CNT",
                "UPDT_DT_TM",
                "UPDT_ID",
                "UPDT_TASK",
                "EVENT_CODE_STATUS_CD",
                "COLLATING_SEQ",
                "LAST_UTC_TS",
                "INST_ID"
        };
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
