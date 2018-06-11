package org.endeavourhealth.transform.barts.cache;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.common.BasisTransformer;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.LocationBuilder;
import org.hl7.fhir.instance.model.Identifier;
import org.hl7.fhir.instance.model.Location;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.endeavourhealth.transform.common.BasisTransformer.createLocationResourceId;
import static org.endeavourhealth.transform.common.BasisTransformer.getLocationResourceId;

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

    public static UUID getOrCreateLocationUUID(BartsCsvHelper csvHelper, CsvCell locationIdCell) throws Exception {
        UUID uuid = getLocationUUID(csvHelper, locationIdCell);

        if (uuid == null) {
            // Create place holder location
            LocationBuilder locationBuilder = createLocationBuilder(locationIdCell);

            locationBuilder.setStatus(Location.LocationStatus.ACTIVE);
            locationBuilder.setMode(Location.LocationMode.INSTANCE);

            IdentifierBuilder identifierBuilder = new IdentifierBuilder(locationBuilder);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_LOCATION_ID);
            identifierBuilder.setUse(Identifier.IdentifierUse.OFFICIAL);
            identifierBuilder.setValue(locationIdCell.getString(), locationIdCell);

            CernerCodeValueRef cernerCodeValueRef = csvHelper.lookupCodeRef(CodeValueSet.LOCATION_NAME, locationIdCell);
            if (cernerCodeValueRef != null) {
                locationBuilder.setName(cernerCodeValueRef.getCodeDispTxt());
            } else {
                locationBuilder.setName("Unknown location");
            }

            uuid = UUID.fromString(locationBuilder.getResourceId());

            locationBuildersByUuid.replace(uuid, locationBuilder);
        }

        return uuid;
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
            BasisTransformer.saveAdminResource(fhirResourceFiler, null, LocationBuilder);
        }
        LOG.trace("Finishing saving " + locationBuildersByUuid.size() + " Locations to the DB");

        //clear down as everything has been saved
        locationBuildersByUuid = new HashMap<>();
    }
}
