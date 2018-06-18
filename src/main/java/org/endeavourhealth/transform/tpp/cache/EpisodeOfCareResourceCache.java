package org.endeavourhealth.transform.tpp.cache;

import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.hl7.fhir.instance.model.EpisodeOfCare;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class EpisodeOfCareResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(EpisodeOfCareResourceCache.class);

    private static Map<Long, EpisodeOfCareBuilder> episodeOfCareBuildersByPatientId = new HashMap<>();


    public static EpisodeOfCareBuilder getOrCreateEpisodeOfCareBuilder(CsvCell rowIdCell, TppCsvHelper csvHelper,
                                                               FhirResourceFiler fhirResourceFiler) throws Exception {

        EpisodeOfCareBuilder episodeOfCareBuilder = episodeOfCareBuildersByPatientId.get(rowIdCell.getLong());
        if (episodeOfCareBuilder == null) {

            EpisodeOfCare EpisodeOfCare
                    = (EpisodeOfCare) csvHelper.retrieveResource(rowIdCell.getString(), ResourceType.EpisodeOfCare, fhirResourceFiler);
            if (EpisodeOfCare == null) {
                //if the Patient doesn't exist yet, create a new one
                 episodeOfCareBuilder = new EpisodeOfCareBuilder();
                episodeOfCareBuilder.setId(rowIdCell.getString(), rowIdCell);
            } else {
                episodeOfCareBuilder = new EpisodeOfCareBuilder(EpisodeOfCare);
            }

            episodeOfCareBuildersByPatientId.put(rowIdCell.getLong(), episodeOfCareBuilder);
        }
        return episodeOfCareBuilder;
    }

    public static EpisodeOfCareBuilder getEpisodeOfCareByRowId(Long rowId) {
        return episodeOfCareBuildersByPatientId.get(rowId);
    }

    public static void removeEpisodeOfCareByPatientId(Long id) {
        episodeOfCareBuildersByPatientId.remove(id);
    }

    public static boolean episodeOfCareInCache(Long rowId) {
        return episodeOfCareBuildersByPatientId.containsKey(rowId);
    }

    public static int size() {
        return episodeOfCareBuildersByPatientId.size();
    }

    public static void listRemaining() {
        for (Long rowId : episodeOfCareBuildersByPatientId.keySet()) {
            EpisodeOfCareBuilder episodeOfCareBuilder = episodeOfCareBuildersByPatientId.get(rowId);
            LOG.info(episodeOfCareBuilder.toString());
        }
    }

    public static void clear() {
        episodeOfCareBuildersByPatientId.clear();
    }
}
