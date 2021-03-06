package org.endeavourhealth.transform.adastra.csv.schema;

import org.endeavourhealth.transform.adastra.AdastraCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;

import java.util.UUID;

public class CASE extends AbstractCsvParser {

    public CASE(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                    "PatientRef",
                    "PriorityName",
                    "CaseRef",
                    "CaseNo",
                    "StartDateTime",
                    "EndDateTime",
                    "LocationName"
            };
        }
        else {

            return new String[]{
                    "PatientRef",
                    "PriorityName",
                    "CaseRef",
                    "CaseNo",
                    "StartDateTime",
                    "EndDateTime",
                    "LocationName",
                    "CaseTagName",
                    "ArrivedPCC",
                    "UserRef",
                    "ODSCode"
            };
        }
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getPatientId() {
        return super.getCell("PatientRef");
    }

    public CsvCell getPriorityName() {
        return super.getCell("PriorityName");
    }

    public CsvCell getCaseId() {
        return super.getCell("CaseRef");
    }

    public CsvCell getCaseNo() {
        return super.getCell("CaseNo");
    }

    public CsvCell getStartDateTime() {
        return super.getCell("StartDateTime");
    }

    public CsvCell getEndDateTime() {
        return super.getCell("EndDateTime");
    }

    public CsvCell getLocationName() {
        return super.getCell("LocationName");
    }

    //version 2 additional

    public CsvCell getCaseTagName() {
        return super.getCell("CaseTagName");
    }

    public CsvCell getArrivedPCC() {
        return super.getCell("ArrivedPCC");
    }

    public CsvCell getUserRef() {
        return super.getCell("UserRef");
    }

    public CsvCell getODSCode() {
        return super.getCell("ODSCode"); }
}
