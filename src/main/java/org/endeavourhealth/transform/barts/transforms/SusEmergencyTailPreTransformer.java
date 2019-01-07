package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.transform.barts.cache.SusTailCacheEntry;
import org.endeavourhealth.transform.barts.schema.SusEmergencyTail;
import org.endeavourhealth.transform.common.CsvCell;

import java.util.Map;

public class SusEmergencyTailPreTransformer {

    /**
     * simply caches the contents of a tails file into a hashmap
     */
    public static void transform(SusEmergencyTail parser, Map<String, SusTailCacheEntry> tailsCache) throws Exception {

        //don't catch any record level parsing errors, since any problems here need to stop the transform
        while (parser.nextRecord()) {
            processRecord(parser, tailsCache);
        }
    }

    private static void processRecord(SusEmergencyTail parser, Map<String, SusTailCacheEntry> tailsCache) {

        //only cache the fields we know we'll need
        CsvCell finNumber = parser.getFinNumber();
        CsvCell encounterId = parser.getEncounterId();
        CsvCell episodeId = parser.getEpisodeId();
        CsvCell personId = parser.getPersonId();
        CsvCell personnelId = parser.getResponsiblePersonnelId();

        SusTailCacheEntry obj = new SusTailCacheEntry();
        obj.setFinNumber(finNumber);
        obj.setEncounterId(encounterId);
        obj.setEpisodeId(episodeId);
        obj.setPersonId(personId);
        obj.setResponsibleHcpPersonnelId(personnelId);

        CsvCell uniqueId = parser.getCdsUniqueId();
        tailsCache.put(uniqueId.getString(), obj);
    }
}
