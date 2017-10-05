package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.transform.barts.schema.Tails;
import org.endeavourhealth.transform.barts.schema.TailsRecord;
import org.endeavourhealth.transform.common.exceptions.TransformException;

import java.util.HashMap;

public class TailsPreTransformer {

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
                tr.setEncounterId(parser.getEncounterId());
                hm.put(tr.getCDSUniqueueId(), tr);
            } catch (Exception ex) {
                throw new TransformException(parser.getCurrentState().toString(), ex);
            }
        }
    }

    public static TailsRecord getTailsRecord(String s) {
        return hm.get(s);
    }

}
