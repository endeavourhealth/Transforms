package org.endeavourhealth.transform.barts.cache;

import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.BasisTransformer;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.LocationBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.EpisodeOfCareBuilder;
import org.hl7.fhir.instance.model.Location;
import org.hl7.fhir.instance.model.EpisodeOfCare;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.endeavourhealth.transform.common.BasisTransformer.*;

public class LocationResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(LocationResourceCache.class);

    private static Map<UUID, LocationBuilder> locationBuildersByUuid = new HashMap<>();
    private static Map<String, UUID> locationIdToUuid = new HashMap<>();

    public static LocationBuilder getLocationBuilder(BartsCsvHelper csvHelper, CsvCell locationIdCell) throws Exception {

        ResourceId locationResourceId = getLocationResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, locationIdCell.getString());

        if (locationResourceId == null) {
            return null;
        }

        LocationBuilder locationBuilder = locationBuildersByUuid.get(locationResourceId.getResourceId());

        if (locationBuilder == null) {

            Location location = (Location)csvHelper.retrieveResource(ResourceType.Location, locationResourceId.getResourceId());
            if (location != null) {
                locationBuilder = new LocationBuilder(location);
                locationBuildersByUuid.put(locationResourceId.getResourceId(), locationBuilder);
                locationIdToUuid.put(locationIdCell.getString(), UUID.fromString(locationBuilder.getResourceId()));
            }

        }

        return locationBuilder;
    }

    public static UUID getLocationUUID(BartsCsvHelper csvHelper, CsvCell locationIdCell) throws Exception {

        UUID locationResourceUUID = locationIdToUuid.get(locationIdCell.getString());

        if (locationResourceUUID != null) {
            return locationResourceUUID;
        }

        ResourceId locationResourceId = getLocationResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, locationIdCell.getString());

        if (locationResourceId == null) {
            return null;
        } else {
            locationIdToUuid.put(locationIdCell.getString(), locationResourceId.getResourceId());
            return locationResourceId.getResourceId();
        }
    }


    public static LocationBuilder createLocationBuilder(CsvCell locationIdCell) throws Exception {

        ResourceId locationResourceId = getLocationResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, locationIdCell.getString());

        if (locationResourceId == null) {
            locationResourceId = createLocationResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, locationIdCell.getString());
        }

        LocationBuilder locationBuilder = new LocationBuilder();
        locationBuilder.setId(locationResourceId.getResourceId().toString(), locationIdCell);

        locationBuildersByUuid.put(locationResourceId.getResourceId(), locationBuilder);
        locationIdToUuid.put(locationIdCell.getString(), UUID.fromString(locationBuilder.getResourceId()));

        return locationBuilder;
    }

    public static void fileLocationResources(FhirResourceFiler fhirResourceFiler) throws Exception {

        LOG.trace("Saving " + locationBuildersByUuid.size() + " Locations to the DB");
        for (UUID LocationId: locationBuildersByUuid.keySet()) {
            LocationBuilder LocationBuilder = locationBuildersByUuid.get(LocationId);
            BasisTransformer.savePatientResource(fhirResourceFiler, null, LocationBuilder);
        }
        LOG.trace("Finishing saving " + locationBuildersByUuid.size() + " Locations to the DB");

        //clear down as everything has been saved
        locationBuildersByUuid = new HashMap<>();
    }
}
