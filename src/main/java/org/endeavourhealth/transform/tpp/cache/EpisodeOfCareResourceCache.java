package org.endeavourhealth.transform.tpp.cache;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.BasisTransformer;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PatientBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.hl7.fhir.instance.model.EpisodeOfCare;
import org.hl7.fhir.instance.model.Patient;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class EpisodeOfCareResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(EpisodeOfCareResourceCache.class);

    private static Map<Long, EpisodeOfCareBuilder> EpisodeOfCareBuildersByPatientId = new HashMap<>();

    public static EpisodeOfCareBuilder getOrCreateEpisodeOfCareBuilder(CsvCell rowIdCell, TppCsvHelper csvHelper,
                                                               FhirResourceFiler fhirResourceFiler) throws Exception {

        EpisodeOfCareBuilder episodeOfCareBuilder = EpisodeOfCareBuildersByPatientId.get(rowIdCell.getLong());
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

            EpisodeOfCareBuildersByPatientId.put(rowIdCell.getLong(), episodeOfCareBuilder);
        }
        return episodeOfCareBuilder;
    }

    public static EpisodeOfCareBuilder getEpisodeOfCareByRowId(Long rowId) {
        return EpisodeOfCareBuildersByPatientId.get(rowId);
    }

    public static void removeEpisodeOfCareByPatientId(Long id) {
        EpisodeOfCareBuildersByPatientId.remove(id);
    }

    public static boolean episodeOfCareInCache(Long rowId) {
        return EpisodeOfCareBuildersByPatientId.containsKey(rowId);
    }

    public static int size() {
        return EpisodeOfCareBuildersByPatientId.size();
    }

    public static void listRemaining() {
        for (Long rowId : EpisodeOfCareBuildersByPatientId.keySet()) {
            EpisodeOfCareBuilder episodeOfCareBuilder = EpisodeOfCareBuildersByPatientId.get(rowId);
            LOG.info(episodeOfCareBuilder.toString());
        }
    }

    public static void clear() {
        EpisodeOfCareBuildersByPatientId.clear();
    }
}
