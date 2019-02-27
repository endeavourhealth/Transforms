package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.cache.SusPatientTailCache;
import org.endeavourhealth.transform.barts.cache.SusTailCacheEntry;
import org.endeavourhealth.transform.barts.schema.SusInpatientTail;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ParserI;

import java.util.List;

public class SusInpatientTailPreTransformer {

      public static void transform(List<ParserI> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {

            while (parser.nextRecord()) {
                try {
                    processRecord((org.endeavourhealth.transform.barts.schema.SusInpatientTail)parser, csvHelper);

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

    }
    private static void processRecord(SusInpatientTail parser,BartsCsvHelper csvHelper) {
        SusPatientTailCache tailCache = csvHelper.getSusPatientTailCache();
        //only cache the fields we know we'll need
        CsvCell encounterId = parser.getEncounterId();
        CsvCell episodeId = parser.getEpisodeId();
        CsvCell personId = parser.getPersonId();
        CsvCell personnelId = parser.getResponsiblePersonnelId();
        CsvCell cdsUniqueId = parser.getCdsUniqueId();
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
        CsvCell uniqueId = parser.getCdsUniqueId();

        tailCache.cacheRecord(obj);
    }
}
