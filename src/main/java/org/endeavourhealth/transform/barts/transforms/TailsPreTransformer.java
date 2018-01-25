package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.transform.barts.schema.Tails;
import org.endeavourhealth.transform.barts.schema.TailsRecord;
import org.endeavourhealth.core.exceptions.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class TailsPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(TailsPreTransformer.class);

    public static HashMap<String, TailsRecord> hm = new HashMap<String, TailsRecord>();

    public static void transform(String version,
                                 Tails parser) throws Exception {

        //unlike most of the other parsers, we don't handle record-level exceptions and continue, since a failure
        //to parse any record in this file it a critical error

        hm.clear();

        while (parser.nextRecord()) {

            try {
                TailsRecord tr = new TailsRecord();
                tr.setCDSUniqueueId(parser.getCDSUniqueueId());
                tr.setFINNbr(parser.getFINNbr());
                tr.setEncounterId(parser.getEncounterId());
                tr.setEpisodeId(parser.getEpisodeId());
                hm.put(tr.getCDSUniqueueId(), tr);
                //LOG.trace("Adding CDS-Tail:" + tr.getCDSUniqueueId());
            } catch (Exception ex) {
                throw new TransformException(parser.getCurrentState().toString(), ex);
            }
        }
    }

    public static TailsRecord getTailsRecord(String s) {
        //LOG.trace("Looking for CDS-Tail:" + s);
        return hm.get(s);
    }

}
