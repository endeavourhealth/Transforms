package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.cache.SusPatientTailCache;
import org.endeavourhealth.transform.barts.cache.SusTailCacheEntry;
import org.endeavourhealth.transform.barts.schema.SusInpatientTail;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SusInpatientTailPreTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SusPatientTailCache.class);
      public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {
          SusPatientTailCache tailCache = csvHelper.getSusPatientTailCache();
        for (ParserI parser: parsers) {

            while (parser.nextRecord()) {
                try {
                    processRecord((org.endeavourhealth.transform.barts.schema.SusInpatientTail)parser, csvHelper,tailCache);

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    LOG.info("SusPatientTail cache size:" + tailCache.size());
    }
    private static void processRecord(SusInpatientTail parser,BartsCsvHelper csvHelper,SusPatientTailCache tailCache) {

        //only cache the fields we know we'll need
        CsvCell encounterId = parser.getEncounterId();
        CsvCell episodeId = parser.getEpisodeId();
        CsvCell personId = parser.getPersonId();
        CsvCell personnelId = parser.getResponsiblePersonnelId();
        CsvCell cdsUniqueId = parser.getCdsUniqueId();
        CsvCell cdsActivityDate = parser.getCdsActivityDate();
        int seqNo=1; // Primary is default
        if (tailCache.encIdInCache(encounterId.getString())) {
            List<SusTailCacheEntry> s = tailCache.getPatientByEncId(encounterId.getString());
            seqNo = s.size() + 1;
        }

        SusTailCacheEntry obj = new SusTailCacheEntry();
        obj.setCDSUniqueIdentifier(cdsUniqueId);
        obj.setEncounterId(encounterId);
        obj.setEpisodeId(episodeId);
        obj.setPersonId(personId);
        obj.setResponsibleHcpPersonnelId(personnelId);
        obj.setSeqNo(seqNo);
        obj.setCdsActivityDate(cdsActivityDate);
        CsvCell uniqueId = parser.getCdsUniqueId();

        tailCache.cacheRecord(obj);
    }
}
