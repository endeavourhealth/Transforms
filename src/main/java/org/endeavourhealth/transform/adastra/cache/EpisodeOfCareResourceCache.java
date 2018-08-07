package org.endeavourhealth.transform.adastra.cache;

import org.endeavourhealth.transform.adastra.AdastraCsvHelper;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ResourceCache;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.hl7.fhir.instance.model.EpisodeOfCare;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EpisodeOfCareResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(EpisodeOfCareResourceCache.class);

    private ResourceCache<String, EpisodeOfCareBuilder> episodeOfCareBuildersByCaseId = new ResourceCache<>();

    public EpisodeOfCareBuilder getOrCreateEpisodeOfCareBuilder(CsvCell caseIdCell,
                                                                       AdastraCsvHelper csvHelper,
                                                                       FhirResourceFiler fhirResourceFiler) throws Exception {

        EpisodeOfCareBuilder cachedResource
                = episodeOfCareBuildersByCaseId.getAndRemoveFromCache(caseIdCell.getString());
        if (cachedResource != null) {
            return cachedResource;
        }

        EpisodeOfCareBuilder episodeOfCareBuilder = null;

        EpisodeOfCare episodeOfCare
                    = (EpisodeOfCare) csvHelper.retrieveResource(caseIdCell.getString(), ResourceType.EpisodeOfCare, fhirResourceFiler);
        if (episodeOfCare == null) {
            //if the Patient episode doesn't exist yet, create a new one using the Case Id
            episodeOfCareBuilder = new EpisodeOfCareBuilder();
            episodeOfCareBuilder.setId(caseIdCell.getString(), caseIdCell);
        } else {
            episodeOfCareBuilder = new EpisodeOfCareBuilder(episodeOfCare);
        }

        return episodeOfCareBuilder;
    }

    public void cleanUpResourceCache() {
        try {
            episodeOfCareBuildersByCaseId.clear();
        } catch (Exception ex) {
            LOG.error("Error cleaning up cache", ex);
        }
    }

    public void returnEpisodeOfCareBuilder(CsvCell caseIdCell, EpisodeOfCareBuilder episodeOfCareBuilder) throws Exception {
        returnEpisodeOfCareBuilder(caseIdCell.getString(), episodeOfCareBuilder);
    }

    public void returnEpisodeOfCareBuilder(String caseId, EpisodeOfCareBuilder episodeOfCareBuilder) throws Exception {
        episodeOfCareBuildersByCaseId.addToCache(caseId, episodeOfCareBuilder);
    }


}
