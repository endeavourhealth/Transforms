package org.endeavourhealth.transform.adastra.csv.schema;

import org.endeavourhealth.transform.adastra.AdastraCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;

import java.util.UUID;

public class USERS extends AbstractCsvParser {

    public USERS(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                    "UserRef",
                    "UserName",
                    "Forename",
                    "Surname",
                    "FullName",
                    "ProviderRef",
                    "ProviderName",
                    "ProviderType",
                    "ProviderGMC",
                    "ProviderNMC"
            };
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getUserRef() {
        return super.getCell("UserRef");
    }

    public CsvCell getUserName() {
        return super.getCell("UserName");
    }

    public CsvCell getForename() {
        return super.getCell("Forename");
    }

    public CsvCell getSurname() {
        return super.getCell("Surname");
    }

    public CsvCell getFullName() {
        return super.getCell("FullName");
    }

    public CsvCell getProviderRef() {
        return super.getCell("ProviderRef");
    }

    public CsvCell getProviderName() {
        return super.getCell("ProviderName");
    }

    public CsvCell getProviderType() {
        return super.getCell("ProviderType");
    }

    public CsvCell getProviderGMC() {
        return super.getCell("ProviderGMC");
    }

    public CsvCell getProviderNMC() {
        return super.getCell("ProviderNMC");
    }
}