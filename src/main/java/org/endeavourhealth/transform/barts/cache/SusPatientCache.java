package org.endeavourhealth.transform.barts.cache;

import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;


public class SusPatientCache {
    private static final Logger LOG = LoggerFactory.getLogger(SusPatientCache.class);


    private final BartsCsvHelper csvHelper;
    private HashMap<String, SusPatientCacheEntry> patientCacheByCdsUniqueId = new HashMap<>();

    public SusPatientCache(BartsCsvHelper csvHelper) {
        this.csvHelper = csvHelper;
    }

    public SusPatientCacheEntry getPatientByCdsUniqueId(String id) {
        return patientCacheByCdsUniqueId.get(id);
    }


    public boolean csdUIdInCache(String id) {
        return patientCacheByCdsUniqueId.containsKey(id);
    }



    /**
     * if we have had an error that's caused us to drop out of the transform, we can call this to tidy up
     * anything we've saved to the audit.queued_message table
     */
    public void cleanUpResourceCache() {
        try {
            patientCacheByCdsUniqueId.clear();
        } catch (Exception ex) {
            LOG.error("Error cleaning up cache", ex);
        }
    }

    public int size() {
        return patientCacheByCdsUniqueId.size();
    }

}
