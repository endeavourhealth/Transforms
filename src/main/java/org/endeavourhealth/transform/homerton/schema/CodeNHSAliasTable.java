package org.endeavourhealth.transform.homerton.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.homerton.HomertonCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class CodeNHSAliasTable extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(CodeNHSAliasTable.class);

    public CodeNHSAliasTable(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                HomertonCsvToFhirTransformer.CSV_FORMAT,
                HomertonCsvToFhirTransformer.DATE_FORMAT,
                HomertonCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "CODE_VALUE",
                "CONTRIBUTOR_SOURCE_CD",
                "CONTRIBUTOR_SOURCE",
                "ALIAS_TYPE_MEANING",
                "CODE_SET",
                "ALIAS",
                "UPDT_DT_TM",
                "UPDT_ID",
                "UPDT_TASK",
                "UPDT_CNT",
                "UPDT_APPLCTX",
                "LAST_UPDATED",
                "IMPORT_ID",
                "HASH"
        };
    }

    public CsvCell getCodeValueCode() {
        return super.getCell("CODE_VALUE");
    }

    public CsvCell getCodeValueNHSAlias() {
        return super.getCell("ALIAS");
    }


    @Override
    protected boolean isFileAudited() {
        return true;
    }
}