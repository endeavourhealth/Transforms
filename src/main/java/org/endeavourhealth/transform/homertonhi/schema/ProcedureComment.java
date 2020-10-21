package org.endeavourhealth.transform.homertonhi.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.homertonhi.HomertonHiCsvToFhirTransformer;

import java.util.UUID;

public class ProcedureComment extends AbstractCsvParser {

    public ProcedureComment(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
        HomertonHiCsvToFhirTransformer.CSV_FORMAT,
        HomertonHiCsvToFhirTransformer.DATE_FORMAT,
        HomertonHiCsvToFhirTransformer.TIME_FORMAT);
    }

    //@Override
    protected String[] getCsvHeaders(String version) {

            return new String[] {
                    "POPULATION_ID",
                    "EMPI_ID",
                    "PROCEDURE_ID",
                    "COMMENT_TEXT",
                    "COMMENT_DT_TM",
                    "COMMENT_DATE_ID",
                    "COMMENT_PROVIDER_ID",
                    "SOURCE_TYPE",
                    "SOURCE_ID",
                    "SOURCE_VERSION",
                    "SOURCE_DESCRIPTION",
                    "COMMENT_TIME_ID",
                    "COMMENT_DATE",
                    "HASH_VALUE"
            };
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getProcedureId() {
        return super.getCell("PROCEDURE_ID");
    }

    public CsvCell getPersonEmpiId() {
        return super.getCell("EMPI_ID");
    }

    public CsvCell getProcedureCommentText() { return super.getCell("COMMENT_TEXT");  }

    public CsvCell getHashValue() { return super.getCell("HASH_VALUE"); }
}

