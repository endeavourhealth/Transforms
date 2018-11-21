package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class DOCRP extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(DOCRP.class);

    public DOCRP(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                BartsCsvToFhirTransformer.DATE_FORMAT,
                BartsCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[] {
                "#DOC_RESPONSE_KEY",
                "EXTRACT_DT_TM",
                "ACTIVE_IND",
                "ENCNTR_ID",
                "PERSON_ID",
                "DOC_INPUT_ID",
                "ELEMENT_EVENT_ID",
                "FORM_EVENT_ID",
                "SECTION_EVENT_ID",
                "GRID_EVENT_ID",
                "ROW_EVENT_ID",
                "FORM_STATUS_CD",
                "DOCUMENTATION_DT_TM",
                "FIRST_DOCUMENTED_DT_TM",
                "LAST_DOCUMENTED_DT_TM",
                "PERFORMED_PRSNL_ID",
                "PERFORMED_DT_TM",
                "RESPONSE_VALUE_TXT",
                "NUMERIC_RESPONSE_NBR",
                "RESPONSE_DT_TM",
                "STRING_RESPONSE_TXT",
                "RESPONSE_NOMEN_ID",
                "RESPONSE_CODE_VALUE_CD"
        };
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
