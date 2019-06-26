package org.endeavourhealth.transform.adastra.csv.schema;

import org.endeavourhealth.transform.adastra.AdastraCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;

import java.util.UUID;

public class ElectronicPrescriptions extends AbstractCsvParser {

    public ElectronicPrescriptions(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                    "ProviderRef",
                    "SubmissionTime",
                    "AuthorisationTime",
                    "CancellationRequestedTime",
                    "CancellationConfirmationTime",
                    "CancellationReasonCode",
                    "CancellationReasonText",
                    "PharmacyName",
                    "PrescriberNumber ",
                    "PrescriberName"
            };
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getCaseId() {
        return super.getCell("CaseRef");
    }

    public CsvCell getProviderRef() {
        return super.getCell("ProviderRef");
    }

    public CsvCell getSubmissionTime() {
        return super.getCell("SubmissionTime");
    }

    public CsvCell getAuthorisationTime() {
        return super.getCell("AuthorisationTime");
    }

    public CsvCell getCancellationRequestedTime() {
        return super.getCell("CancellationRequestedTime");
    }

    public CsvCell getCancellationConfirmationTime() {
        return super.getCell("CancellationConfirmationTime");
    }

    public CsvCell getCancellationReasonCode() {
        return super.getCell("CancellationReasonCode");
    }

    public CsvCell getCancellationReasonText() {
        return super.getCell("CancellationReasonText");
    }

    public CsvCell getPharmacyName() {
        return super.getCell("PharmacyName");
    }

    public CsvCell getPrescriberNumber() {
        return super.getCell("PrescriberNumber");
    }

    public CsvCell getPrescriberName() {
        return super.getCell("PrescriberName");
    }
}
