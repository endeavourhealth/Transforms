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
import org.endeavourhealth.transform.common.resourceValidators.ResourceValidatorLocation;
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
        LocationResourceCache.serviceId = serviceId;
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
        LOG.info("Filing location resources. Count : " + LocationBuildersByRowId.size());
        for (Long rowId: LocationBuildersByRowId.keySet()) {
            LocationBuilder locationBuilder = LocationBuildersByRowId.get(rowId);
            boolean mapIds = !locationBuilder.isIdMapped();
            ResourceValidatorLocation validator = new ResourceValidatorLocation();
            List<String> errors = new ArrayList<>();
            Location loc = (Location) locationBuilder.getResource();
            if (loc != null) {
//                validator.validateResourceSave(locationBuilder.getResource(), serviceId, mapIds, errors);
//                if (!errors.isEmpty()) {
//                    //TODO remove debugging LOGs
//                    LOG.info("Validation errors for Location:" + locationBuilder.getResourceId());
//                    for (String s: errors) {
//                        LOG.info(s);
//                    }
//                    Resource resource = locationBuilder.getResource();
//                    BaseIdMapper idMapper = IdHelper.getIdMapper(resource);
//                    Set<String> referenceValues = new HashSet<>();
//                    idMapper.getResourceReferences(resource, referenceValues);
//                    for (String referenceValue: referenceValues) {
//                        Reference reference = ReferenceHelper.createReference(referenceValue);
//                        ReferenceComponents comps = ReferenceHelper.getReferenceComponents(reference);
//                        String referenceId = comps.getId();
//                        boolean mapRefIds = !ResourceValidatorBase.isReferenceIdMapped(reference, serviceId);
//                        if (mapRefIds != mapIds) {
//                            LOG.info("Mapping ref id for Location " + referenceId);
//                            if (!mapRefIds) {
//                                reference = IdHelper.convertEdsReferenceToLocallyUniqueReference(fhirResourceFiler,reference);
//                            } else {
//                                reference = IdHelper.convertLocallyUniqueReferenceToEdsReference(reference, fhirResourceFiler);
//                            }
//                        } else {
//                            LOG.info("referenceId " + referenceId + " isRefMapped" +mapRefIds );
//                        }
//                    }
//                }
                fhirResourceFiler.saveAdminResource(null, mapIds, locationBuilder);
            } else {
                LOG.info("No Location resource found for LocationBuilder" + rowId);
            }
        }

        //clear down as everything has been saved
        LocationBuildersByRowId.clear();
    }
}
