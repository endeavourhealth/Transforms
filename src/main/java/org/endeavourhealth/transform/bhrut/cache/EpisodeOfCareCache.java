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

    public EpisodeOfCareBuilder getOrCreateEpisodeOfCareBuilder(CsvCell caseIdCell,
                                                                BhrutCsvHelper csvHelper,
                                                                FhirResourceFiler fhirResourceFiler) throws Exception {

        EpisodeOfCareBuilder cachedResource
                = episodeOfCareBuildersByCaseId.getAndRemoveFromCache(caseIdCell.getString());
        if (cachedResource != null) {
            return cachedResource;
        }

        EpisodeOfCareBuilder episodeOfCareBuilder = null;

        EpisodeOfCare episodeOfCare
                = (EpisodeOfCare) csvHelper.retrieveResource(caseIdCell.getString(), ResourceType.EpisodeOfCare);
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
