package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;


public class ProcedurePojoCache {
    private static final Logger LOG = LoggerFactory.getLogger(ProcedurePojoCache.class);

    public static final String TWO_DECIMAL_PLACES = ".00";

    private final BartsCsvHelper csvHelper;
    private HashMap<String, List<ProcedurePojo>> procedurePojosByEncounterId = new HashMap<>();

    public ProcedurePojoCache(BartsCsvHelper csvHelper) {
        this.csvHelper = csvHelper;
    }

    public List<ProcedurePojo> getProcedurePojoByEncId(String id) {
        if (id.endsWith(TWO_DECIMAL_PLACES)) {
            return procedurePojosByEncounterId.get(id);
        } else {
            return procedurePojosByEncounterId.get(id + TWO_DECIMAL_PLACES);
        }
    }

    // Remember if you're calling this that "Procedure" enc ids end ".00". I assume people are going to forget
    // this and try to use just encounter ids so added a little fallback. I considered removing it
    // but left in case they sort out the format and remove the trailing .00.
    public ProcedurePojo getProcedurePojoByMultipleFields(String encId, String procCd, Date date) throws Exception {
        if (procIdInCache(encId)) {
            for (ProcedurePojo pojo : getProcedurePojoByEncId(encId)) {
                try {
                    if (pojo.getProcedureCode().getString().equalsIgnoreCase(procCd) &&
                            pojo.getProc_dt_tm().getDate().equals(date)) {
                        return pojo;
                    }
                } catch (Exception exception) {
                    continue; // CsvCell handles logging so try next result
                }
            }
        }
        return null;
    }

    public boolean procIdInCache(String id) {
        if (id.endsWith(TWO_DECIMAL_PLACES)) {
            return procedurePojosByEncounterId.containsKey(id);
        } else {
            return procedurePojosByEncounterId.containsKey(id + TWO_DECIMAL_PLACES);
        }
    }

    public void cachePojo(ProcedurePojo pojo) {
        String id = pojo.getEncounterId().getString();
        if (procIdInCache(id)) {
            List<ProcedurePojo> list = getProcedurePojoByEncId(id);
            list.add(pojo);
        } else {
            List<ProcedurePojo> list = new ArrayList<>();
            list.add(pojo);
            procedurePojosByEncounterId.put(id, list);
        }
    }

    /**
     * if we have had an error that's caused us to drop out of the transform, we can call this to tidy up
     * anything we've saved to the audit.queued_message table
     */
    public void cleanUpResourceCache() {
        try {
            procedurePojosByEncounterId.clear();
        } catch (Exception ex) {
            LOG.error("Error cleaning up cache", ex);
        }
    }

}
