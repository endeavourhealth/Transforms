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
    private HashMap<String, List<SusTailCacheEntry>> patientTailCacheByEncId = new HashMap<>();

    public SusPatientTailCache(BartsCsvHelper csvHelper) {
        this.csvHelper = csvHelper;
    }

    public List<SusTailCacheEntry> getPatientByEncId(String id) {
        return patientTailCacheByEncId.get(id);
    }

    public boolean encIdInCache(String id) {
        return patientTailCacheByEncId.containsKey(id);
    }

    public void cacheRecord(SusTailCacheEntry record) {
        String id= record.getCDSUniqueIdentifier().getString();

        if (encIdInCache(id)) {
            List<SusTailCacheEntry> list = getPatientByEncId(id);
            list.add(record);
        } else {
            List<SusTailCacheEntry> list = new ArrayList<>();
            list.add(record);
            patientTailCacheByEncId.put(id, list);
        }
    }



    /**
     * if we have had an error that's caused us to drop out of the transform, we can call this to tidy up
     * anything we've saved to the audit.queued_message table
     */
    public void cleanUpResourceCache() {
        try {
            patientTailCacheByEncId.clear();
        } catch (Exception ex) {
            LOG.error("Error cleaning up cache", ex);
        }
    }

    public int size() {
        return patientTailCacheByEncId.size();
    }

}
