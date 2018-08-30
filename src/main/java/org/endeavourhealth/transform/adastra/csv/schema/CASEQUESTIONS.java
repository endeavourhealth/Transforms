package org.endeavourhealth.transform.adastra.csv.schema;

import org.endeavourhealth.transform.adastra.AdastraCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;

import java.util.UUID;

public class CASEQUESTIONS extends AbstractCsvParser {

    public CASEQUESTIONS(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                AdastraCsvToFhirTransformer.CSV_FORMAT.withHeader(getExpectedCsvHeaders(version)),
                AdastraCsvToFhirTransformer.DATE_FORMAT,
                AdastraCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return getExpectedCsvHeaders(version);
    }

    private static String[] getExpectedCsvHeaders(String version) {
        return new String[]{
                "CaseRef",
                "QuestionSetName",
                "Question",
                "Answer",
                "SortOrder"
        };
    }

    @Override
    protected String getFileTypeDescription() {
        return "Adastra Case Questions file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getCaseId() {
        return super.getCell("CaseRef");
    }

    public CsvCell getQuestionSetName() {
        return super.getCell("QuestionSetName");
    }

    public CsvCell getQuestion() {
        return super.getCell("Question");
    }

    public CsvCell getAnswer() {
        return super.getCell("Answer");
    }

    public CsvCell getSortOrder() {
        return super.getCell("SortOrder");
    }
}
