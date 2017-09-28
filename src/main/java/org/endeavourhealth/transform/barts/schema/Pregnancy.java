package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.AbstractFixedParser;
import org.endeavourhealth.transform.barts.FixedParserField;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;

import java.io.File;

public class Pregnancy extends AbstractFixedParser {

    public Pregnancy(String version, File f, boolean openParser) throws Exception {
        super(version, f, openParser, EmisCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD, EmisCsvToFhirTransformer.TIME_FORMAT);

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