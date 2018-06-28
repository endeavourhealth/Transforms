package org.endeavourhealth.transform.adastra.cache;

import org.endeavourhealth.transform.adastra.AdastraCsvHelper;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.hl7.fhir.instance.model.EpisodeOfCare;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class EpisodeOfCareResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(EpisodeOfCareResourceCache.class);

    private static Map<String, EpisodeOfCareBuilder> EpisodeOfCareBuildersByCaseId = new HashMap<>();

    public static EpisodeOfCareBuilder getOrCreateEpisodeOfCareBuilder(CsvCell caseIdCell,
                                                                       AdastraCsvHelper csvHelper,
                                                                       FhirResourceFiler fhirResourceFiler) throws Exception {

        EpisodeOfCareBuilder episodeOfCareBuilder = EpisodeOfCareBuildersByCaseId.get(caseIdCell.getString());
        if (episodeOfCareBuilder == null) {

            EpisodeOfCare episodeOfCare
                    = (EpisodeOfCare) csvHelper.retrieveResource(caseIdCell.getString(), ResourceType.EpisodeOfCare, fhirResourceFiler);
            if (episodeOfCare == null) {
                //if the Patient episode doesn't exist yet, create a new one using the Case Id
                episodeOfCareBuilder = new EpisodeOfCareBuilder();
                episodeOfCareBuilder.setId(caseIdCell.getString(), caseIdCell);
            } else {
                episodeOfCareBuilder = new EpisodeOfCareBuilder(episodeOfCare);
            }

            EpisodeOfCareBuildersByCaseId.put(caseIdCell.getString(), episodeOfCareBuilder);
        }
        return episodeOfCareBuilder;
    }

    public static int size() {
        return EpisodeOfCareBuildersByCaseId.size();
    }

    public static void clear() {
        EpisodeOfCareBuildersByCaseId.clear();
    }
}
