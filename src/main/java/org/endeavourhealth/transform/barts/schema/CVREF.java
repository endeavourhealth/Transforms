package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class CVREF extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(CVREF.class);

    public CVREF(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                null, null); //all Barts date parsing for Power Insight content should use BartsCsvHelper.parseDate(..)
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "#CODE_VALUE_CD",
                "EXTRACT_DT_TM",
                "ACTIVE_IND",
                "CODE_DESC_TXT",
                "CODE_DISP_TXT",
                "CODE_MEANING_TXT",
                "CODE_SET_NBR",
                "CODE_SET_DESC_TXT",
                "ALIAS_NHS_CD_ALIAS"
        };
    }

    public CsvCell getCodeValueCode() {
        return super.getCell("#CODE_VALUE_CD");
    }

    public CsvCell getExtractDate() {
        return super.getCell("EXTRACT_DT_TM");
    }

    public CsvCell getActiveInd() {
        return super.getCell("ACTIVE_IND");
    }

    public CsvCell getCodeDescTxt() {
        return super.getCell("CODE_DESC_TXT");
    }

    public CsvCell getCodeDispTxt() {
        return super.getCell("CODE_DISP_TXT");
    }

    public CsvCell getCodeMeaningTxt() {
        return super.getCell("CODE_MEANING_TXT");
    }

    public CsvCell getCodeSetNbr() {
        return super.getCell("CODE_SET_NBR");
    }

    public CsvCell getCodeSetDescTxt() {
        return super.getCell("CODE_SET_DESC_TXT");
    }

    public CsvCell getAliasNhsCdAlias() {
        return super.getCell("ALIAS_NHS_CD_ALIAS");
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}