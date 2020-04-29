package org.endeavourhealth.transform.adastra.cache;

import org.endeavourhealth.transform.adastra.AdastraCsvHelper;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ResourceCache;
import org.endeavourhealth.transform.common.resourceBuilders.LocationBuilder;
import org.hl7.fhir.instance.model.Location;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocationResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(LocationResourceCache.class);

    private ResourceCache<String, LocationBuilder> locationBuildersByID = new ResourceCache<>();

    public LocationBuilder getOrCreateLocationBuilder(String locationId,
                                                              AdastraCsvHelper csvHelper,
                                                              FhirResourceFiler fhirResourceFiler,
                                                              AbstractCsvParser parser) throws Exception {

        LocationBuilder cachedResource
                = locationBuildersByID.getAndRemoveFromCache(locationId);
        if (cachedResource != null) {
            return cachedResource;
        }

        LocationBuilder locationBuilder = null;

        Location location
                = (Location) csvHelper.retrieveResource(locationId, ResourceType.Location, fhirResourceFiler);
        if (location == null) {

            //if the Location resource doesn't exist yet, create a new one using the location code
            locationBuilder = new LocationBuilder();
            locationBuilder.setId(locationId);

        } else {
            locationBuilder = new LocationBuilder(location);
        }

        return locationBuilder;
    }

    public boolean locationInCache(String locationId) {
        return locationBuildersByID.contains(locationId);
    }

    public boolean locationInDB(String locationId, AdastraCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception  {
        return (csvHelper.retrieveResource(locationId, ResourceType.Location, fhirResourceFiler) != null);
    }

    public void returnLocationBuilder(String locationId, LocationBuilder locationBuilder) throws Exception {
        locationBuildersByID.addToCache(locationId, locationBuilder);
    }

    public void removeOrganizationFromCache(String locationId) throws Exception {
        locationBuildersByID.removeFromCache(locationId);
    }

    public void cleanUpResourceCache() {
        try {
            locationBuildersByID.clear();
        } catch (Exception ex) {
            LOG.error("Error cleaning up cache", ex);
        }
    }
}
