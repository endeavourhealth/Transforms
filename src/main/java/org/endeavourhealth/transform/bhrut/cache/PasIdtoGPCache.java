package org.endeavourhealth.transform.bhrut.cache;

import org.endeavourhealth.transform.common.CsvCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PasIdtoGPCache {
    // Basic org code or org name cache
    private static final Logger LOG = LoggerFactory.getLogger(StaffCache.class);

    private Map<String, String> pasIdtoGPOrg = new ConcurrentHashMap<>();

    public void addGpCode(CsvCell pasIdCell, CsvCell orgIdCell) {

        String code = orgIdCell.getString();
        String key = pasIdCell.getString();
        if (!pasIdtoGPOrg.containsKey(key)) {
            pasIdtoGPOrg.put(key, code);
        }
    }

    public boolean pasIdInCache(String key) {
        return pasIdtoGPOrg.containsKey(key);
    }

    public String getGpCodeforPasId(String key) {
        if (!pasIdInCache(key)) {
            return null;
        }
        return pasIdtoGPOrg.get(key);
    }

    public void empty() {
        pasIdtoGPOrg.clear();
    }

    public long size() {
        return pasIdtoGPOrg.size();
    }
}
