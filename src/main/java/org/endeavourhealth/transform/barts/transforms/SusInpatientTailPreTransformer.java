package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.transform.barts.cache.SusTailCacheEntry;
import org.endeavourhealth.transform.barts.schema.SusInpatientTail;
import org.endeavourhealth.transform.common.CsvCell;

import java.util.Map;

public class SusInpatientTailPreTransformer {

    /**
     * simply caches the contents of a tails file into a hashmap
     */
    public static void transform(SusInpatientTail parser, Map<String, SusTailCacheEntry> tailsCache) throws Exception {

        //don't catch any record level parsing errors, since any problems here need to stop the transform
        while (parser.nextRecord()) {
            processRecord(parser, tailsCache);
        }
    }

    private static void processRecord(SusInpatientTail parser, Map<String, SusTailCacheEntry> tailsCache) {

        //only cache the fields we know we'll need
        CsvCell encounterId = parser.getEncounterId();
        CsvCell episodeId = parser.getEpisodeId();
        CsvCell personId = parser.getPersonId();
        CsvCell personnelId = parser.getResponsiblePersonnelId();
        CsvCell cdsUniqueId = parser.getCdsUniqueId();

        SusTailCacheEntry obj = new SusTailCacheEntry();
        obj.setCDSUniqueIdentifier(cdsUniqueId);
        obj.setEncounterId(encounterId);
        obj.setEpisodeId(episodeId);
        obj.setPersonId(personId);
        obj.setResponsibleHcpPersonnelId(personnelId);

        CsvCell uniqueId = parser.getCdsUniqueId();
        tailsCache.put(uniqueId.getString(), obj);
    }
}
