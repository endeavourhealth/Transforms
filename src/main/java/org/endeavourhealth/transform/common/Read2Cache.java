package org.endeavourhealth.transform.common;

import org.endeavourhealth.common.utility.ExpiringCache;
import org.endeavourhealth.common.utility.ExpiringSet;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.reference.Read2ToSnomedMapDalI;
import org.endeavourhealth.core.terminology.Read2Code;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Read2Cache {
    private static final Logger LOG = LoggerFactory.getLogger(Read2Cache.class);

    private static final long CACHE_DURATION = 1000 * 60 * 60; //cache objects for 2hrs

    private static Map<String, StringMemorySaver> hmRead2CodeToTerm = new ExpiringCache<>(CACHE_DURATION);
    private static Set<String> hsNonValidRead2Codes = new ExpiringSet<>(CACHE_DURATION);

    public static boolean isRealRead2Code(String code) throws Exception {
        String term = lookUpRead2TermForCode(code);
        return term != null;
    }

    public static String lookUpRead2TermForCode(String code) throws Exception {

        StringMemorySaver cached = hmRead2CodeToTerm.get(code);
        if (cached != null) {
            return cached.toString();
        }

        if (hsNonValidRead2Codes.contains(code)) {
            return null;
        }

        //LOG.trace("Going to DB to check [" + code + "]");
        Read2ToSnomedMapDalI dal = DalProvider.factoryRead2ToSnomedMapDal();
        Read2Code read2Code = dal.getRead2Code(code);
        //LOG.trace("Result for [" + code + "] -> " + read2Code);

        //add to one of the caches so we can avoid the same lookup for a while
        if (read2Code == null) {
            hsNonValidRead2Codes.add(code);
            return null;

        } else {
            String ret = read2Code.getPreferredTerm();
            hmRead2CodeToTerm.put(code, new StringMemorySaver(ret));
            return ret;
        }
    }

}
