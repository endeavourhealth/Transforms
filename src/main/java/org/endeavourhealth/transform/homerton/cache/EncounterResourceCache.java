package org.endeavourhealth.transform.homerton.cache;

import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.ResourceCache;
import org.endeavourhealth.transform.common.referenceLists.ReferenceList;
import org.endeavourhealth.transform.common.resourceBuilders.ContainedListBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EncounterBuilder;
import org.endeavourhealth.transform.homerton.HomertonCsvHelper;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EncounterResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(EncounterResourceCache.class);

    private ResourceCache<String, EncounterBuilder> encounterBuildersByEncounterId = new ResourceCache<>();

    public EncounterResourceCache() {};

    public EncounterBuilder getEncounterBuilder(CsvCell encounterIdCell, HomertonCsvHelper csvHelper) throws Exception {

        String encounterId = encounterIdCell.getString();

        //check the cache
        EncounterBuilder cachedResource = encounterBuildersByEncounterId.getAndRemoveFromCache(encounterId);
        if (cachedResource != null) {
            return cachedResource;
        }

        EncounterBuilder encounterBuilder = null;

        //if not in the cache, check the DB
        Encounter encounter = (Encounter)csvHelper.retrieveResourceForLocalId(ResourceType.Encounter, encounterId);
        if (encounter != null) {

            encounterBuilder = new EncounterBuilder(encounter);

            //apply any newly linked child resources (observations, procedures etc.)
            ContainedListBuilder containedListBuilder = new ContainedListBuilder(encounterBuilder);
            ReferenceList newLinkedResources = csvHelper.getAndRemoveNewConsultationRelationships(encounterIdCell);
            if (newLinkedResources != null) {
                //LOG.debug("Encounter " + encounterId + " has " + newLinkedResources.size() + " child resources");
                for (int i=0; i<newLinkedResources.size(); i++) {
                    Reference reference = newLinkedResources.getReference(i);
                    CsvCell[] sourceCells = newLinkedResources.getSourceCells(i);
                    //note we need to convert the reference from a local ID one to a Discovery UUID one
                    reference = IdHelper.convertLocallyUniqueReferenceToEdsReference(reference, csvHelper);
                    containedListBuilder.addReference(reference, sourceCells);
                }
            }
        } else {

            encounterBuilder = new EncounterBuilder();
            encounterBuilder.setId(encounterIdCell.getString(), encounterIdCell);

            //apply any newly linked child resources (observations, procedures etc.)
            ContainedListBuilder containedListBuilder = new ContainedListBuilder(encounterBuilder);
            ReferenceList newLinkedResources = csvHelper.getAndRemoveNewConsultationRelationships(encounterIdCell);
            if (newLinkedResources != null) {
                //LOG.debug("Encounter " + encounterId + " has " + newLinkedResources.size() + " child resources");
                containedListBuilder.addReferences(newLinkedResources);
            }
        }

        return encounterBuilder;
    }

    public void returnEncounterBuilder(CsvCell encounterIdCell, EncounterBuilder encounterBuilder) throws Exception {
        String encounterId = encounterIdCell.getString();
        encounterBuildersByEncounterId.addToCache(encounterId, encounterBuilder);
    }

    public void removeEncounterFromCache(CsvCell encounterIdCell) throws Exception {
        String encounterId = encounterIdCell.getString();
        encounterBuildersByEncounterId.removeFromCache(encounterId);
    }

    public void cleanUpResourceCache() {
        try {
            encounterBuildersByEncounterId.clear();
        } catch (Exception ex) {
            LOG.error("Error cleaning up cache", ex);
        }
    }
}
