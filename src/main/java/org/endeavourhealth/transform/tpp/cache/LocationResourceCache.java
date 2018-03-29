package org.endeavourhealth.transform.tpp.cache;

import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.LocationBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.hl7.fhir.instance.model.Location;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class LocationResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(LocationResourceCache.class);

    private static Map<Long, LocationBuilder> LocationBuildersByRowId = new HashMap<>();

    public static LocationBuilder getLocationBuilder(CsvCell rowIdCell, TppCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {

        LocationBuilder LocationBuilder = LocationBuildersByRowId.get(rowIdCell.getLong());
        if (LocationBuilder == null) {

            Location Location
                    = (Location)csvHelper.retrieveResource(rowIdCell.getString(), ResourceType.Location, fhirResourceFiler);
            if (Location == null) {
                //if the Location doesn't exist yet, create a new one
                LocationBuilder = new LocationBuilder();
                LocationBuilder.setId(rowIdCell.getString(), rowIdCell);
            } else {
                LocationBuilder = new LocationBuilder(Location);
            }
            LocationBuildersByRowId.put(rowIdCell.getLong(), LocationBuilder);
        }
        return LocationBuilder;
    }

      public static boolean LocationInCache(CsvCell rowIdCell)  {
        return LocationBuildersByRowId.containsKey(rowIdCell.getLong());
    }

    public static void fileLocationResources(FhirResourceFiler fhirResourceFiler) throws Exception {

        for (Long rowId: LocationBuildersByRowId.keySet()) {
            LocationBuilder LocationBuilder = LocationBuildersByRowId.get(rowId);
            fhirResourceFiler.saveAdminResource(null, LocationBuilder);
        }

        //clear down as everything has been saved
        LocationBuildersByRowId.clear();
    }
}
