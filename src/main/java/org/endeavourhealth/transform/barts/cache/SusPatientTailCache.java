package org.endeavourhealth.transform.barts.cache;

import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class SusPatientTailCache {
    private static final Logger LOG = LoggerFactory.getLogger(SusPatientTailCache.class);

    private final BartsCsvHelper csvHelper;
    private HashMap<String, List<SusTailCacheEntry>> patientTailCacheByCSDUniquecId = new HashMap<>();

    public SusPatientTailCache(BartsCsvHelper csvHelper) {
        this.csvHelper = csvHelper;
    }

    public List<SusTailCacheEntry> getPatientByUniqueId(String id) {
        return patientTailCacheByCSDUniquecId.get(id);
    }

    public boolean CSDuniqueIdInCache(String id) {
        return patientTailCacheByCSDUniquecId.containsKey(id);
    }

    public void cacheRecord(SusTailCacheEntry record) {
        String id= record.getCDSUniqueIdentifier().getString();

        if (CSDuniqueIdInCache(id)) {
            List<SusTailCacheEntry> list = getPatientByUniqueId(id);
            list.add(record);
        } else {
            List<SusTailCacheEntry> list = new ArrayList<>();
            list.add(record);
            patientTailCacheByCSDUniquecId.put(id, list);
        }
    }



    /**
     * if we have had an error that's caused us to drop out of the transform, we can call this to tidy up
     * anything we've saved to the audit.queued_message table
     */
    public void cleanUpResourceCache() {
        try {
            patientTailCacheByCSDUniquecId.clear();
        } catch (Exception ex) {
            LOG.error("Error cleaning up cache", ex);
        }
    }

    public int size() {
        return patientTailCacheByCSDUniquecId.size();
    }

}
