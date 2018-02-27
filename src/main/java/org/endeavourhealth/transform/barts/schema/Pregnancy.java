package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.common.AbstractFixedParser;
import org.endeavourhealth.transform.common.FixedParserField;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;

public class Pregnancy extends AbstractFixedParser {

    public Pregnancy(String version, String filePath, boolean openParser) throws Exception {
        super(version, filePath, openParser, EmisCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD, EmisCsvToFhirTransformer.TIME_FORMAT);

        addFieldList(new FixedParserField("CDSVersion",             1, 6));
        addFieldList(new FixedParserField("CDSRecordType",          7, 3));
        addFieldList(new FixedParserField("CDSReplacementgroup",    10, 3));
        addFieldList(new FixedParserField("MRN",    284, 10));
        addFieldList(new FixedParserField("DOB",    321, 8));
    }

    public String getLocalPatientId() {
        return super.getString("MRN");
    }
    public String getDOB() {
        return super.getString("DOB");
    }


}