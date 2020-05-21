package org.endeavourhealth.transform.bhrut.cache;

import org.endeavourhealth.transform.common.CsvCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OrgCache {
    // Basic org code or org name cache
    private static final Logger LOG = LoggerFactory.getLogger(StaffCache.class);

    private Map<String, String> orgCodeToName = new ConcurrentHashMap<>();

    public void addOrgCode(CsvCell orgCodeCell, CsvCell orgNameCell) {

        String cCode = orgCodeCell.getString();
        String name = orgNameCell.getString();
        if (!orgCodeToName.containsKey(cCode)) {
            orgCodeToName.put(cCode, name);
        }
    }

    public boolean orgCodeInCache(String cCode) {
        return orgCodeToName.containsKey(cCode);
    }

    public String getNameForOrgCode(String cCode) {
        if (!orgCodeInCache(cCode)) {
            return null;
        }
        return orgCodeToName.get(cCode);
    }

    public void empty() {
        orgCodeToName.clear();
    }

    public long size() {
        return orgCodeToName.size();
    }
}
