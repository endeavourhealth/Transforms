package org.endeavourhealth.transform.adastra.csv.schema;

import org.endeavourhealth.transform.adastra.AdastraCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;

import java.util.UUID;

public class NOTES extends AbstractCsvParser {

    public NOTES(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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

        if (version.equalsIgnoreCase(AdastraCsvToFhirTransformer.VERSION_1)) {

            return new String[]{
                    "CaseRef",
                    "PatientRef",
                    "ReviewDateTime",
                    "NoteText",
                    "Obsolete",
                    "Active"
            };
        } else {

            return new String[]{
                    "CaseRef",
                    "PatientRef",
                    "ReviewDateTime",
                    "NoteText",
                    "Obsolete",
                    "Active",
                    "UserRef"
            };
        }
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getCaseId() {
        return super.getCell("CaseRef");
    }

    public CsvCell getPatientId() {
        return super.getCell("PatientRef");
    }

    public CsvCell getReviewDateTime() {
        return super.getCell("ReviewDateTime");
    }

    public CsvCell getNoteText() {
        return super.getCell("NoteText");
    }

    public CsvCell getObsolete() {
        return super.getCell("Obsolete");
    }

    public CsvCell getActive() {
        return super.getCell("Active");
    }

    //version 2 additional

    public CsvCell getUserRef() {
        return super.getCell("UserRef");
    }
}
