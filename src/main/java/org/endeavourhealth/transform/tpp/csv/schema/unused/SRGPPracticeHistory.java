package org.endeavourhealth.transform.tpp.csv.schema.unused;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.tpp.TppCsvToFhirTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SRGPPracticeHistory extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(SRGPPracticeHistory.class);

    public SRGPPracticeHistory(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                TppCsvToFhirTransformer.CSV_FORMAT,
                TppCsvToFhirTransformer.DATE_FORMAT,
                TppCsvToFhirTransformer.TIME_FORMAT,
                TppCsvToFhirTransformer.ENCODING);
    }


    @Override
    protected String[] getCsvHeaders(String version) {
        if (version.equals(TppCsvToFhirTransformer.VERSION_TEST_PACK_2)) {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "IDPractice",
                    "IDProfileRegisteredGP",
                    "DateFrom",
                    "DateTo",
                    "IDPatient"
            };
        } else {
            return new String[]{
                    "RowIdentifier",
                    "IDOrganisationVisibleTo",
                    "IDPractice",
                    "IDProfileRegisteredGP",
                    "DateFrom",
                    "DateTo",
                    "IDPatient",
                    "RemovedData"

            };
        }

    }

    public CsvCell getRowIdentifier() {
        return super.getCell("RowIdentifier");
    }

    public CsvCell getIDOrganisationVisibleTo() {
        return super.getCell("IDOrganisationVisibleTo");
    }

    public CsvCell getIDPractice() {
        return super.getCell("IDPractice");
    }

    public CsvCell getIDProfileRegisteredGP() {
        return super.getCell("IDProfileRegisteredGP");
    }

    public CsvCell getDateFrom() {
        return super.getCell("DateFrom");
    }

    public CsvCell getDateTo() {
        return super.getCell("DateTo");
    }

    public CsvCell getIDPatient() {
        return super.getCell("IDPatient");
    }

    public CsvCell getRemovedData() {
        return super.getCell("RemovedData");
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
