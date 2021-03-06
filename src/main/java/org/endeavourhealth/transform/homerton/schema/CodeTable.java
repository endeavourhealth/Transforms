package org.endeavourhealth.transform.homerton.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.homerton.HomertonCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class CodeTable extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(CodeTable.class);

    public CodeTable(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                HomertonCsvToFhirTransformer.CSV_FORMAT,
                HomertonCsvToFhirTransformer.DATE_FORMAT,
                HomertonCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "CODE_VALUE",
                "CODE_SET",
                "CDF_MEANING",
                "DISPLAY",
                "DISPLAY_KEY",
                "DESCRIPTION",
                "DEFINITION",
                "COLLATION_SEQ",
                "ACTIVE_TYPE_CD",
                "ACTIVE_IND",
                "ACTIVE_DT_TM",
                "INACTIVE_DT_TM",
                "UPDT_DT_TM",
                "UPDT_ID",
                "UPDT_CNT",
                "UPDT_TASK",
                "UPDT_APPLCTX",
                "BEGIN_EFFECTIVE_DT_TM",
                "END_EFFECTIVE_DT_TM",
                "DATA_STATUS_CD",
                "DATA_STATUS_DT_TM",
                "DATA_STATUS_PRSNL_ID",
                "ACTIVE_STATUS_PRSNL_ID",
                "CKI",
                "DISPLAY_KEY_NLS",
                "CONCEPT_CKI",
                "DISPLAY_KEY_A_NLS",
                "LAST_UPDATED",
                "IMPORT_ID",
                "HASH"
        };
    }

    public CsvCell getCodeValueCode() {
        return super.getCell("CODE_VALUE");
    }

    public CsvCell getActiveInd() {
        return super.getCell("ACTIVE_IND");
    }

    public CsvCell getDate() {
        return super.getCell("UPDT_DT_TM");
    }

    public CsvCell getCodeMeaningTxt() {
        return super.getCell("CDF_MEANING");
    }

    public CsvCell getCodeDispTxt() {
        return super.getCell("DISPLAY");
    }

    public CsvCell getCodeDescTxt() {
        return super.getCell("DESCRIPTION");
    }

    public CsvCell getCodeSetNbr() {
        return super.getCell("CODE_SET");
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}