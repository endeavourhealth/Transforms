package org.endeavourhealth.transform.common;

import org.endeavourhealth.common.utility.ExpiringCache;
import org.endeavourhealth.common.utility.ExpiringSet;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.reference.SnomedDalI;
import org.endeavourhealth.core.database.dal.reference.models.SnomedLookup;
import org.endeavourhealth.core.terminology.SnomedCode;
import org.endeavourhealth.core.terminology.TerminologyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class SnomedCache {
    private static final Logger LOG = LoggerFactory.getLogger(SnomedCache.class);

    private static final long CACHE_DURATION = 1000 * 60 * 60; //cache objects for 2hrs

    private static Map<Long, StringMemorySaver> hmSnomedConceptToTerm = new ExpiringCache<>(CACHE_DURATION);
    private static Set<Long> hsNonValidConcepts = new ExpiringSet<>(CACHE_DURATION);

    public static String lookUpSnomedTermForConcept(Long conceptId) throws Exception {

        StringMemorySaver cached = hmSnomedConceptToTerm.get(conceptId);
        if (cached != null) {
            return cached.toString();
        }

        if (hsNonValidConcepts.contains(conceptId)) {
            return null;
        }

        //LOG.trace("Going to DB to check [" + conceptId + "]");
        SnomedDalI dal = DalProvider.factorySnomedDal();
        SnomedLookup snomedLookup = dal.getSnomedLookup("" + conceptId);
        //LOG.trace("Result for [" + conceptId + "] -> " + snomedCode);

        if (snomedLookup == null) {
            hsNonValidConcepts.add(conceptId);
            return null;

        } else {
            String ret = snomedLookup.getTerm();
            hmSnomedConceptToTerm.put(conceptId, new StringMemorySaver(ret));
            return ret;
        }
    }
}
