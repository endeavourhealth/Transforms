package org.endeavourhealth.transform.vision.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.vision.VisionCsvToFhirTransformer;

import java.util.UUID;

public class Practice extends AbstractCsvParser {

    public Practice(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                VisionCsvToFhirTransformer.CSV_FORMAT.withHeader(getHeaders(version)),
                VisionCsvToFhirTransformer.DATE_FORMAT,
                VisionCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return getHeaders(version);
    }

    private static String[] getHeaders(String version) {
        //test pack has the same columns as live
        return new String[]{
                "ID",
                "NAME",
                "IDENTIFIER",   //odsCode
                "HA",
                "ADDRESS",
                "ADDRESS_1",
                "ADDRESS_2",
                "ADDRESS_3",
                "ADDRESS_4",
                "ADDRESS_5",
                "POSTCODE",
                "PHONE",
                "EMAIL",
                "FAX"
        };
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getOrganisationID() {
        return super.getCell("ID");
    }

    public CsvCell getOrganisationName() {
        return super.getCell("NAME");
    }

    public CsvCell getIdentifier() {
        return super.getCell("IDENTIFIER");
    }

    public CsvCell getHA() {
        return super.getCell("HA");
    }

    public CsvCell getAddressAll() {
        return super.getCell("ADDRESS");
    }

    public CsvCell getAddress1() {
        return super.getCell("ADDRESS_1");
    }

    public CsvCell getAddress2() {
        return super.getCell("ADDRESS_2");
    }

    public CsvCell getAddress3() {
        return super.getCell("ADDRESS_3");
    }

    public CsvCell getAddress4() {
        return super.getCell("ADDRESS_4");
    }

    public CsvCell getAddress5() {
        return super.getCell("ADDRESS_5");
    }

    public CsvCell getPostCode() {
        return super.getCell("POSTCODE");
    }

    public CsvCell getPhone() {
        return super.getCell("PHONE");
    }

    public CsvCell getEmail() {
        return super.getCell("EMAIL");
    }

    public CsvCell getFax() {
        return super.getCell("FAX");
    }


}
