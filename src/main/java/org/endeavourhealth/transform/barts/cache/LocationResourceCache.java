package org.endeavourhealth.transform.barts.cache;

import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.LocationBuilder;
import org.hl7.fhir.instance.model.Location;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class LocationResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(LocationResourceCache.class);

    private final BartsCsvHelper csvHelper;
    private Map<String, LocationBuilder> locationBuildersById = new HashMap<>();
    private Map<String, LocationBuilder> placeholderLocationBuildersById = new HashMap<>();
    private Map<String, Boolean> locationsCheckedOnDb = new HashMap<>();

    public LocationResourceCache(BartsCsvHelper csvHelper) {
        this.csvHelper = csvHelper;
    }

    public void cacheLocationBuilder(LocationBuilder locationBuilder) {

        String id = locationBuilder.getResourceId();
        locationBuildersById.put(id, locationBuilder);

        //if the placeholders map contains this ID, then discard it
        placeholderLocationBuildersById.remove(id);
    }

    public void cachePlaceholderLocationBuilder(LocationBuilder locationBuilder) throws Exception {

        String id = locationBuilder.getResourceId();

        //if the location builder already exists in the proper map, then discard this placeholder
        if (locationBuildersById.containsKey(id)) {
            return;
        }

        //check the DB to see if we have a location resource already, if so discard this placeholder
        Boolean existsOnDb = locationsCheckedOnDb.get(id);
        if (existsOnDb == null) {
            Location existingResource = (Location)csvHelper.retrieveResourceForLocalId(ResourceType.Location, id);
            existsOnDb = new Boolean(existingResource != null);
            locationsCheckedOnDb.put(id, existsOnDb);
        }
        if (existsOnDb.booleanValue()) {
            return;
        }

        placeholderLocationBuildersById.put(id, locationBuilder);
    }


    public void fileLocationResources(FhirResourceFiler fhirResourceFiler) throws Exception {

        //save the proper locations
        LOG.trace("Saving " + locationBuildersById.size() + " Locations to the DB");
        for (String id: locationBuildersById.keySet()) {
            LocationBuilder locationBuilder = locationBuildersById.get(id);
            fhirResourceFiler.saveAdminResource(null, locationBuilder);
        }
        LOG.trace("Finishing saving " + locationBuildersById.size() + " Locations to the DB");

        //save the placeholder locations we've generated
        LOG.trace("Saving " + placeholderLocationBuildersById.size() + " placeholder Locations to the DB");
        for (String id: placeholderLocationBuildersById.keySet()) {
            LocationBuilder locationBuilder = placeholderLocationBuildersById.get(id);
            fhirResourceFiler.saveAdminResource(null, locationBuilder);
        }
        LOG.trace("Finishing saving " + placeholderLocationBuildersById.size() + " placeholder Locations to the DB");

        //clear down as everything has been saved
        locationBuildersById.clear();
        placeholderLocationBuildersById.clear();
        locationsCheckedOnDb.clear();
    }


}
