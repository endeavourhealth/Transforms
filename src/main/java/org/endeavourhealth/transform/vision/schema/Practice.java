package org.endeavourhealth.transform.vision.schema;

import org.endeavourhealth.transform.emis.csv.schema.AbstractCsvParser;
import org.endeavourhealth.transform.vision.VisionCsvToFhirTransformer;

public class Practice extends AbstractCsvParser {

    public Practice(String version, String filePath, boolean openParser) throws Exception {
        super(version, filePath, openParser, VisionCsvToFhirTransformer.CSV_FORMAT.withHeader(
                "ID",
                "NAME",
                "IDENTIFIER",
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
                "FAX"),
                VisionCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD,
                VisionCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
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

    public String getOrganisationID() {
        return super.getString("ID");
    }
    public String getOrganisationName() { return super.getString("NAME"); }
    public String getIdentifier() {
        return super.getString("IDENTIFIER");
    }
    public String getHA() {
        return super.getString("HA");
    }
    public String getAddressAll () { return super.getString("ADDRESS"); }
    public String getAddress1 () { return super.getString("ADDRESS_1"); }
    public String getAddress2 () { return super.getString("ADDRESS_2"); }
    public String getAddress3 () { return super.getString("ADDRESS_3"); }
    public String getAddress4 () { return super.getString("ADDRESS_4"); }
    public String getAddress5 () { return super.getString("ADDRESS_5"); }
    public String getPostCode() {
        return super.getString("POSTCODE");
    }
    public String getPhone() {
        return super.getString("PHONE");
    }
    public String getEmail() {
        return super.getString("EMAIL");
    }
    public String getFax () { return super.getString("FAX");
    }

}
