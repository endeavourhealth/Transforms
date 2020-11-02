package org.endeavourhealth.transform.bhrut.cache;

import org.endeavourhealth.transform.bhrut.BhrutCsvHelper;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ResourceCache;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.hl7.fhir.instance.model.EpisodeOfCare;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EpisodeOfCareCache {

    private static final Logger LOG = LoggerFactory.getLogger(EpisodeOfCareCache.class);

    private ResourceCache<String, EpisodeOfCareBuilder> episodeOfCareBuildersByCaseId = new ResourceCache<>();

    public EpisodeOfCareBuilder getOrCreateEpisodeOfCareBuilder(CsvCell idCell,
                                                                BhrutCsvHelper csvHelper,
                                                                FhirResourceFiler fhirResourceFiler) throws Exception {

        EpisodeOfCareBuilder cachedResource
                = episodeOfCareBuildersByCaseId.getAndRemoveFromCache(idCell.getString());
        if (cachedResource != null) {
            return cachedResource;
        }

        EpisodeOfCareBuilder episodeOfCareBuilder = null;

        EpisodeOfCare episodeOfCare
                = (EpisodeOfCare) csvHelper.retrieveResource(idCell.getString(), ResourceType.EpisodeOfCare);
        if (episodeOfCare == null) {
            //if the Patient episode doesn't exist yet, create a new one using the Case Id
            episodeOfCareBuilder = new EpisodeOfCareBuilder();
            episodeOfCareBuilder.setId(idCell.getString(), idCell);
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

    public void cacheEpisodeOfCareBuilder(CsvCell idCell, EpisodeOfCareBuilder episodeOfCareBuilder) throws Exception {
        cacheEpisodeOfCareBuilder(idCell.getString(), episodeOfCareBuilder);
    }

    public void cacheEpisodeOfCareBuilder(String id, EpisodeOfCareBuilder episodeOfCareBuilder) throws Exception {
        episodeOfCareBuildersByCaseId.addToCache(id, episodeOfCareBuilder);
    }
}
