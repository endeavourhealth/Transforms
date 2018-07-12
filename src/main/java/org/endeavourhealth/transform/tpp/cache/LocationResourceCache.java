package org.endeavourhealth.transform.tpp.cache;

import org.endeavourhealth.common.fhir.ReferenceComponents;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.idmappers.BaseIdMapper;
import org.endeavourhealth.transform.common.resourceBuilders.LocationBuilder;
import org.endeavourhealth.transform.common.resourceValidators.ResourceValidatorAppointment;
import org.endeavourhealth.transform.common.resourceValidators.ResourceValidatorBase;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.hl7.fhir.instance.model.Location;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class LocationResourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(LocationResourceCache.class);

    private static Map<Long, LocationBuilder> LocationBuildersByRowId = new HashMap<>();
    private static UUID serviceId = null;

    public static UUID getServiceId() {
        return serviceId;
    }

    public static void setServiceId(UUID serviceId) {
        serviceId = serviceId;
    }

    public static LocationBuilder getLocationBuilder(CsvCell rowIdCell, TppCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {

        LocationBuilder LocationBuilder = LocationBuildersByRowId.get(rowIdCell.getLong());
        if (LocationBuilder == null) {

            Location Location
                    = (Location) csvHelper.retrieveResource(rowIdCell.getString(), ResourceType.Location, fhirResourceFiler);
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

    public static boolean LocationInCache(CsvCell rowIdCell) {
        return LocationBuildersByRowId.containsKey(rowIdCell.getLong());
    }

    public static void fileLocationResources(FhirResourceFiler fhirResourceFiler) throws Exception {

        for (Long rowId: LocationBuildersByRowId.keySet()) {
            LocationBuilder locationBuilder = LocationBuildersByRowId.get(rowId);
            boolean mapIds = !locationBuilder.isIdMapped();
            if (mapIds) {  //If we want to map Ids then all ids in builder and references should go in unmapped
                Resource resource = locationBuilder.getResource();
                BaseIdMapper idMapper = IdHelper.getIdMapper(resource);
                Set<String> referenceValues = new HashSet<>();
                idMapper.getResourceReferences(resource, referenceValues);
                for (String referenceValue: referenceValues) {
                    Reference reference = ReferenceHelper.createReference(referenceValue);
                    ReferenceComponents comps = ReferenceHelper.getReferenceComponents(reference);
                    String referenceId = comps.getId();
                    boolean isRefMapped = ResourceValidatorBase.isReferenceIdMapped(reference, serviceId);
                    if (!isRefMapped) {
                        reference = IdHelper.convertLocallyUniqueReferenceToEdsReference(reference,fhirResourceFiler);
                    }
                }
            }
            fhirResourceFiler.saveAdminResource(null, mapIds, locationBuilder);
        }

        //clear down as everything has been saved
        LocationBuildersByRowId.clear();
    }
}
