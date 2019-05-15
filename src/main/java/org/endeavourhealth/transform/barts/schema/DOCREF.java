package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class DOCREF extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(DOCREF.class);

    public DOCREF(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                null, null); //all Barts date parsing for Power Insight content should use BartsCsvHelper.parseDate(..)
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
            "#DOC_INPUT_ID",
            "EXTRACT_DT_TM",
            "ACTIVE_IND",
            "FORM_REF_ID",
            "FORM_DESC_TXT",
            "FORM_DEFINITION_TXT",
            "SECTION_REF_ID",
            "SECTION_DESC_TXT",
            "SECTION_DEF_TXT",
            "TASK_ASSAY_ID",
            "ELEMENT_DESC_TXT",
            "ELEMENT_MNEMONIC_TXT",
            "ELEMENT_LABEL_TXT",
            "GRID_NAME_TXT",
            "GRID_COLUMN_TASK_ASSAY_ID",
            "GRID_COLUMN_DESC_TXT",
            "GRID_COLUMN_MNEMONIC_TXT",
            "GRID_ROW_TASK_ASSAY_ID",
            "GRID_ROW_DESC_TXT",
            "GRID_ROW_MNEMONIC_TXT",
            "FORM_INSTANCE_ID",
            "GRID_EVENT_CD",
            "INPUT_TYPE_FLG"
        };
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
