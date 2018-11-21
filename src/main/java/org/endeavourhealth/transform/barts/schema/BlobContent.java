package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class BlobContent extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(BlobContent.class);

    public BlobContent(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                "CE_BLOB_CONTENT_KEY",
                "HEALTH_SYSTEM_ID",
                "HEALTH_SYSTEM_SOURCE_ID",
                "CE_BLOB_CONTENT_ID",
                "CLINICAL_EVENT_KEY",
                "EVENT_ID",
                "BLOB_CONTENTS",
                "BLOB_LENGTH",
                "BLOB_SEQ_NBR",
                "BLOB_CLASS_REF",
                "SOURCE_FLG",
                "EXTRACT_DT_TM",
                "UPDT_DT_TM",
                "UPDT_TASK",
                "UPDT_USER",
                "ORPHAN_FLG",
                "ERROR_IND",
                "TRUNCATION_IND",
                "FIRST_PROCESS_DT_TM",
                "LAST_PROCESS_DT_TM",
                "TOTAL_UPDATES"
        };
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
