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
                    "population_id",
                    "empi_id",
                    "procedure_id",
                    "comment_text",
                    "comment_dt_tm",
                    "comment_date_id",
                    "comment_provider_id",
                    "source_type",
                    "source_id",
                    "source_version",
                    "source_description",
                    "comment_time_id",
                    "comment_date",
                    "hash_value"
            };
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getProcedureId() {
        return super.getCell("procedure_id");
    }

    public CsvCell getPersonEmpiId() {
        return super.getCell("empi_id");
    }

    public CsvCell getProcedureCommentText() { return super.getCell("comment_text");  }

    public CsvCell getHashValue() { return super.getCell("hash_value"); }
}