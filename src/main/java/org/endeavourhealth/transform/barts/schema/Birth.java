package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.common.AbstractFixedParser;
import org.endeavourhealth.transform.common.FixedParserField;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Birth extends AbstractFixedParser {

    public Birth(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, EmisCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD, EmisCsvToFhirTransformer.TIME_FORMAT);
    }

    public String getLocalPatientId() {
        return super.getString("MRN");
    }
    public String getDOB() {
        return super.getString("DOB");
    }


    @Override
    protected boolean isFileAudited() {
        return true;
    }

    @Override
    protected boolean skipFirstRow() {
        return true;
    }

    @Override
    protected List<FixedParserField> getFieldList(String version) {

        List<FixedParserField> ret = new ArrayList<>();

        ret.add(new FixedParserField("CDSVersion",             1, 6));
        ret.add(new FixedParserField("CDSRecordType",          7, 3));
        ret.add(new FixedParserField("CDSReplacementgroup",    10, 3));
        ret.add(new FixedParserField("MRN",    284, 10));
        ret.add(new FixedParserField("DOB",    321, 8));

        return ret;
    }


}