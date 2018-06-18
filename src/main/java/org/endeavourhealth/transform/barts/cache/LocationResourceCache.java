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

    private static Map<String, LocationBuilder> locationBuilders = new HashMap<>();
    private static Map<String, LocationBuilder> placeholderLocationBuilders = new HashMap<>();
    private static Map<String, Boolean> locationsCheckedOnDb = new HashMap<>();

    public static void cacheLocationBuilder(LocationBuilder locationBuilder) {

        String id = locationBuilder.getResourceId();
        locationBuilders.put(id, locationBuilder);

        //if the placeholders map contains this ID, then discard it
        placeholderLocationBuilders.remove(id);
    }

    public static void cachePlaceholderLocationBuilder(LocationBuilder locationBuilder, BartsCsvHelper csvHelper) throws Exception {

        String id = locationBuilder.getResourceId();

        //if the location builder already exists in the proper map, then discard this placeholder
        if (locationBuilders.containsKey(id)) {
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

        placeholderLocationBuilders.put(id, locationBuilder);
    }


    public static void fileLocationResources(FhirResourceFiler fhirResourceFiler) throws Exception {

        //save the proper locations
        LOG.trace("Saving " + locationBuilders.size() + " Locations to the DB");
        for (String id: locationBuilders.keySet()) {
            LocationBuilder locationBuilder = locationBuilders.get(id);
            fhirResourceFiler.saveAdminResource(null, locationBuilder);
        }
        LOG.trace("Finishing saving " + locationBuilders.size() + " Locations to the DB");

        //save the placeholder locations we've generated
        LOG.trace("Saving " + placeholderLocationBuilders.size() + " placeholder Locations to the DB");
        for (String id: placeholderLocationBuilders.keySet()) {
            LocationBuilder locationBuilder = placeholderLocationBuilders.get(id);
            fhirResourceFiler.saveAdminResource(null, locationBuilder);
        }
        LOG.trace("Finishing saving " + placeholderLocationBuilders.size() + " placeholder Locations to the DB");

        //clear down as everything has been saved
        locationBuilders.clear();
        placeholderLocationBuilders.clear();
        locationsCheckedOnDb.clear();
    }


}
