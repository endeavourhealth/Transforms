package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.common.AbstractFixedParser;
import org.endeavourhealth.transform.common.FixedParserField;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Tails extends AbstractFixedParser {

    public Tails(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, EmisCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD, EmisCsvToFhirTransformer.TIME_FORMAT);
    }

    public String getCDSUniqueueId() {
        return super.getString("CDSUniqueueId");
    }
    public String getFINNbr() {
        return super.getString("FINNbr");
    }
    public String getEncounterId() {
        return super.getString("EncounterId");
    }
    public String getEpisodeId() { return super.getString("EpisodeId");

    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    @Override
    protected boolean skipFirstRow() {
        return false;
    }

    @Override
    protected List<FixedParserField> getFieldList(String version) {

        List<FixedParserField> ret = new ArrayList<>();
        
        ret.add(new FixedParserField("CDSUniqueueId",             1, 35));
        ret.add(new FixedParserField("FINNbr",          91, 12));
        ret.add(new FixedParserField("EncounterId",          103, 10));
        ret.add(new FixedParserField("EpisodeId",          123, 10));
        // NOTE - fields in the three types of Tails files are identical up to and including field 12        
        return ret;
    }
}