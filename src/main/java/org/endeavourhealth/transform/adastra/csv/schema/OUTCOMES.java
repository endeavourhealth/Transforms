package org.endeavourhealth.transform.adastra.csv.schema;

import org.endeavourhealth.transform.adastra.AdastraCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;

import java.util.UUID;

public class OUTCOMES extends AbstractCsvParser {

    public OUTCOMES(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, AdastraCsvToFhirTransformer.CSV_FORMAT.withHeader(
                "CaseRef",
                "OutcomeName"),
                AdastraCsvToFhirTransformer.DATE_FORMAT,
                AdastraCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {

        return new String[]{
                "CaseRef",
                "OutcomeName"
        };
    }

    @Override
    protected String getFileTypeDescription() {
        return "Adastra Case Outcomes";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getCaseId() { return super.getCell("CaseRef"); }
    public CsvCell getOutcomeName() { return super.getCell("OutcomeName"); }
}
