package org.endeavourhealth.transform.bhrut.schema;

import org.endeavourhealth.transform.bhrut.BhrutCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.vision.VisionCsvToFhirTransformer;

import java.util.UUID;

public class PMI extends AbstractCsvParser {

    public PMI(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BhrutCsvToFhirTransformer.CSV_FORMAT.withHeader(getHeaders(version)),
                BhrutCsvToFhirTransformer.DATE_FORMAT,
                BhrutCsvToFhirTransformer.TIME_FORMAT);
    }
    @Override
    protected boolean isFileAudited() {
        return true;
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return getHeaders(version);
    }

    private static String[] getHeaders(String version) {

            return new String[]{
                    "ID",
                    ""};
            }
    public CsvCell getID() {
        return super.getCell("ID");
    }
        }
