package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.AbstractFixedParser;
import org.endeavourhealth.transform.barts.FixedParserField;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;

import java.io.File;

public class Tails extends AbstractFixedParser {

    public Tails(String version, File f, boolean openParser) throws Exception {
        super(version, f, openParser, EmisCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD, EmisCsvToFhirTransformer.TIME_FORMAT);

        addFieldList(new FixedParserField("CDSUniqueueId",             1, 35));
        addFieldList(new FixedParserField("FINNbr",          91, 12));
        addFieldList(new FixedParserField("EncounterId",          103, 10));
        addFieldList(new FixedParserField("EpisodeId",          123, 10));
        // NOTE - fields in the three types of Tails files are identical up to and including field 12
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

}