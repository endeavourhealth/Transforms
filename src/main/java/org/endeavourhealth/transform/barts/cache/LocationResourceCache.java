package org.endeavourhealth.transform.barts.cache;

import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.FhirResourceFilerI;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.resourceBuilders.LocationBuilder;
import org.hl7.fhir.instance.model.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.endeavourhealth.transform.common.BasisTransformer.*;

public class LocationResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(LocationResourceCache.class);

    //private static Map<UUID, LocationBuilder> locationBuildersByUuid = new HashMap<>();
    private static Map<String, UUID> locationIdMap = new HashMap<String, UUID>();

    public static UUID getEncounterResourceId(String location, BartsCsvHelper csvHelper, FhirResourceFilerI fhirResourceFiler, ParserI parser) throws Exception {
        if (locationIdMap.containsKey(location)) {
            return locationIdMap.get(location);
        } else {
            // Check if location exists but has not been cached
            ResourceId resourceId = getLocationResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, location);
            if (resourceId != null) {
                locationIdMap.put(location, resourceId.getResourceId());
                return resourceId.getResourceId();
            } else {
                CernerCodeValueRef cernerCodeValueRef = csvHelper.lookUpCernerCodeFromCodeSet(CernerCodeValueRef.LOCATION_NAME, Long.getLong(location));
                if (cernerCodeValueRef != null) {
                    // Create place holder location
                    resourceId = new ResourceId();
                    resourceId.setScopeId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE);
                    resourceId.setResourceType("Location");
                    resourceId.setUniqueId("LocationId=" + location);
                    resourceId.setResourceId(UUID.randomUUID());
                    saveResourceId(resourceId);

                    LocationBuilder locationBuilder = new LocationBuilder();
                    locationBuilder.setId(resourceId.getResourceId().toString());
                    locationBuilder.setStatus(Location.LocationStatus.ACTIVE);
                    locationBuilder.setMode(Location.LocationMode.INSTANCE);
                    locationBuilder.setName(cernerCodeValueRef.getCodeDispTxt());
                    fhirResourceFiler.saveAdminResource(parser.getCurrentState(), locationBuilder);

                    locationIdMap.put(location, resourceId.getResourceId());
                    return resourceId.getResourceId();
                } else {
                    // Unknown location
                    return null;
                }
            }
        }
    }


}
