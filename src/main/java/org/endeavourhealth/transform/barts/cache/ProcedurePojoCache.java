package org.endeavourhealth.transform.barts.cache;

import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.schema.ProcedurePojo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class ProcedurePojoCache {
    private static final Logger LOG = LoggerFactory.getLogger(ProcedurePojoCache.class);

    public static final String DUPLICATE_EMERGENCY_PREFIX_SUFFIX = ":EmergencyDuplicate";

    private final BartsCsvHelper csvHelper;
    private static  HashMap<String, ProcedurePojo> procedurePojosByProcedureId = new HashMap<>();


    public ProcedurePojoCache(BartsCsvHelper csvHelper) {
        this.csvHelper = csvHelper;
    }

    public  ProcedurePojo getProcedurePojoByProcId(String id) {
        return procedurePojosByProcedureId.get(id);
    }

    public boolean procIdInCache(String id) {
        return procedurePojosByProcedureId.containsKey(id);
    }

    public  void cachePojo(ProcedurePojo pojo) {
        String id = pojo.getEncounterId().getString();
        procedurePojosByProcedureId.put(id,pojo);
    }

    /**
     * if we have had an error that's caused us to drop out of the transform, we can call this to tidy up
     * anything we've saved to the audit.queued_message table
     */
    public void cleanUpResourceCache() {
        try {
            procedurePojosByProcedureId.clear();
        } catch (Exception ex) {
            LOG.error("Error cleaning up cache", ex);
        }
    }

}
