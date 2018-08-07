package org.endeavourhealth.transform.homerton.cache;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.LocationPhysicalType;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.ResourceCache;
import org.endeavourhealth.transform.common.resourceBuilders.LocationBuilder;
import org.endeavourhealth.transform.homerton.HomertonCsvHelper;
import org.endeavourhealth.transform.homerton.schema.EncounterTable;
import org.hl7.fhir.instance.model.Location;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocationResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(LocationResourceCache.class);

    private ResourceCache<Long, LocationBuilder> locationResourceByLocationId = new ResourceCache<>();


    public LocationBuilder getOrCreateLocationBuilder(CsvCell locationIdCell,
                                                      HomertonCsvHelper csvHelper,
                                                      FhirResourceFiler fhirResourceFiler,
                                                      EncounterTable parser) throws Exception {

        Long locationID = locationIdCell.getLong();

        //check the cache
        LocationBuilder cachedResource = locationResourceByLocationId.getAndRemoveFromCache(locationID);
        if (cachedResource != null) {
            return cachedResource;
        }

        LocationBuilder locationBuilder = null;

        Location location = (Location)csvHelper.retrieveResourceForLocalId(ResourceType.Location, locationID.toString());
        if (location == null) {
            //if the location doesn't exist yet, create a new one
            locationBuilder = new LocationBuilder();
            locationBuilder.setId(locationID.toString());

            CsvCell locationNameCell = parser.getLocationName();
            String locationDisplayName = locationNameCell.getString();
            CsvCell locationBuildingCell = parser.getLocationBuilding();
            CsvCell locationNurseUnitCell = parser.getLocationNurseUnit();
            CsvCell locationRoomCell = parser.getLocationRoom();
            // the location display name is an amalgamation of terms, i.e, Building, Room, Bed
            if (!locationRoomCell.isEmpty()) {
                locationDisplayName = locationRoomCell.getString().concat(", " + locationDisplayName);
            }
            if (!locationNurseUnitCell.isEmpty()) {
                locationDisplayName = locationNurseUnitCell.getString().concat(", " + locationDisplayName);
            }
            if (!locationBuildingCell.isEmpty()) {
                locationDisplayName = locationBuildingCell.getString().concat(", " + locationDisplayName);
            }
            locationBuilder.setName(locationDisplayName, locationNameCell, locationBuildingCell, locationNurseUnitCell, locationRoomCell);

            LocationPhysicalType physicalType = getLocationPhysicalType(parser);
            if (physicalType != null) {
                locationBuilder.setPhysicalType(physicalType);
            }

            // set the Homerton organization reference using the serviceId
            Reference orgReference
                    = ReferenceHelper.createReference(ResourceType.Organization, parser.getServiceId().toString());
            locationBuilder.setManagingOrganisation(orgReference);

            fhirResourceFiler.saveAdminResource(parser.getCurrentState(), locationBuilder);

        } else {

            locationBuilder = new LocationBuilder(location);
        }

        return locationBuilder;
    }

    public LocationPhysicalType getLocationPhysicalType(EncounterTable parser) throws Exception {

        if (!HomertonCsvHelper.isEmptyOrIsZero(parser.getLocationBedCD()))
            return LocationPhysicalType.BED;
        if (!HomertonCsvHelper.isEmptyOrIsZero(parser.getLocationRoomCD()))
            return LocationPhysicalType.ROOM;
        if (!HomertonCsvHelper.isEmptyOrIsZero(parser.getLocationNurseUnitCD()))
            return LocationPhysicalType.NURSEUNIT;
        if (!HomertonCsvHelper.isEmptyOrIsZero(parser.getLocationBuildingCD()))
            return LocationPhysicalType.BUILDING;

        return null;
    }

    public void returnLocationBuilder(CsvCell locationIdCell, LocationBuilder locationBuilder) throws Exception {
        Long locationID = locationIdCell.getLong();
        locationResourceByLocationId.addToCache(locationID, locationBuilder);
    }

    public void removeLocationFromCache(CsvCell locationIdCell) throws Exception {
        Long locationID = locationIdCell.getLong();
        locationResourceByLocationId.removeFromCache(locationID);
    }

    public void cleanUpResourceCache() {
        try {
            locationResourceByLocationId.clear();
        } catch (Exception ex) {
            LOG.error("Error cleaning up cache", ex);
        }
    }
}
