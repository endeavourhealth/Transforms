package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.LocationPhysicalType;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.barts.cache.LocationResourceCache;
import org.endeavourhealth.transform.barts.schema.LOREF;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.FhirResourceFilerI;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.resourceBuilders.LocationBuilder;
import org.hl7.fhir.instance.model.Location;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class LOREFTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(LOREFTransformer.class);


    public static void transform(List<ParserI> parsers, FhirResourceFilerI fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {
                try {
                    createLocation((LOREF)parser, (FhirResourceFiler)fhirResourceFiler, csvHelper);

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }

    /**
     * confusingly, this function can end up creating multiple resources. THe row in the LOREF file does
     * represent a single location, but it also contains enough parent hierarchy info that we
     * can create any missing parent organisations
     */
    public static void createLocation(LOREF parser, FhirResourceFiler fhirResourceFiler, BartsCsvHelper csvHelper) throws Exception {

        //the hierarchy list starts with the "current" location (i.e. the one actually represented by this CSV record)
        //so we always want to create the Location resource for that, but we only want to do the others
        //if we can't find them
        List<IdCellAndPhysicalType> hierarchy = createHierarchy(parser);
        boolean first = true;

        while (!hierarchy.isEmpty()) {

            LocationBuilder locationBuilder = createLocationBuilder(hierarchy, csvHelper);

            if (first) {
                //if the first record, we want to use the dates in the CSV record to work out the active state

                CsvCell endDateCell = parser.getEndEffectiveDateTime();
                if (!BartsCsvHelper.isEmptyOrIsEndOfTime(endDateCell)) {

                    Date endDate = BartsCsvHelper.parseDate(endDateCell);
                    if (endDate.before(new Date())) {
                        locationBuilder.setStatus(Location.LocationStatus.INACTIVE, endDateCell);

                    } else {
                        locationBuilder.setStatus(Location.LocationStatus.ACTIVE, endDateCell);
                    }
                } else {
                    //if no end date, it's active
                    locationBuilder.setStatus(Location.LocationStatus.ACTIVE, endDateCell);
                }

                first = false;

                LocationResourceCache.cacheLocationBuilder(locationBuilder);

            } else {
                //if not the first location in the hierarchy, this is really just a placehold for the proper record, if we find it
                LocationResourceCache.cachePlaceholderLocationBuilder(locationBuilder, csvHelper);
            }
        }
    }

    private static LocationBuilder createLocationBuilder(List<IdCellAndPhysicalType> locationHierarchy, BartsCsvHelper csvHelper) throws Exception {

        IdCellAndPhysicalType o = locationHierarchy.get(0);
        CsvCell locationIdCell = o.getIdCell();
        LocationPhysicalType physicalType = o.getPhysicalType();

        LocationBuilder locationBuilder = new LocationBuilder();
        locationBuilder.setId(locationIdCell.getString(), locationIdCell);

        // Parent location
        if (locationHierarchy.size() > 1) {
            IdCellAndPhysicalType p = locationHierarchy.get(1);
            CsvCell parentIdCell = p.getIdCell();
            Reference parentLocationReference = ReferenceHelper.createReference(ResourceType.Location, parentIdCell.getString());
            locationBuilder.setPartOf(parentLocationReference, parentIdCell);
        }

        // managing org
        String orgId = csvHelper.findOrgRefIdForBarts();
        Reference orgReference = ReferenceHelper.createReference(ResourceType.Organization, orgId);
        locationBuilder.setManagingOrganisation(orgReference);

        // Name
        String name = generateName(csvHelper, locationHierarchy);
        locationBuilder.setName(name, locationIdCell);

        locationBuilder.setPhysicalType(physicalType, locationIdCell);

        //type
        CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.LOCATION_NAME, locationIdCell);
        if (codeRef != null) {
            String typeDesc = codeRef.getCodeMeaningTxt();
            locationBuilder.setTypeFreeText(typeDesc, locationIdCell);
        }

        // Mode
        locationBuilder.setMode(Location.LocationMode.INSTANCE);

        //remove the first entry because we've done this level of the hierarchy
        locationHierarchy.remove(0);

        return locationBuilder;
    }

    private static List<IdCellAndPhysicalType> createHierarchy(LOREF parser) {

        List<IdCellAndPhysicalType> ret = new ArrayList<>();

        CsvCell bedLoc = parser.getBedLocation();
        if (!BartsCsvHelper.isEmptyOrIsZero(bedLoc)) {
            ret.add(new IdCellAndPhysicalType(bedLoc, LocationPhysicalType.BED));
        }

        CsvCell roomLoc = parser.getRoomLocation();
        if (!BartsCsvHelper.isEmptyOrIsZero(roomLoc)) {
            ret.add(new IdCellAndPhysicalType(roomLoc, LocationPhysicalType.ROOM));
        }

        CsvCell nurseUnitLoc = parser.getNurseUnitLocation();
        if (!BartsCsvHelper.isEmptyOrIsZero(nurseUnitLoc)) {
            ret.add(new IdCellAndPhysicalType(nurseUnitLoc, LocationPhysicalType.NURSEUNIT));
        }

        CsvCell ambulatoryLoc = parser.getAmbulatoryLocation();
        if (!BartsCsvHelper.isEmptyOrIsZero(ambulatoryLoc)) {
            ret.add(new IdCellAndPhysicalType(ambulatoryLoc, LocationPhysicalType.AMBULATORY));
        }

        CsvCell surgeryLocationCode = parser.getSurgeryLocation();
        if (!BartsCsvHelper.isEmptyOrIsZero(surgeryLocationCode)) {
            ret.add(new IdCellAndPhysicalType(surgeryLocationCode, LocationPhysicalType.SURGICAL));
        }

        CsvCell buildingLoc = parser.getBuildingLocation();
        if (!BartsCsvHelper.isEmptyOrIsZero(buildingLoc)) {
            ret.add(new IdCellAndPhysicalType(buildingLoc, LocationPhysicalType.BUILDING));
        }

        CsvCell facilityLoc = parser.getFacilityLocation();
        if (!BartsCsvHelper.isEmptyOrIsZero(facilityLoc)) {
            ret.add(new IdCellAndPhysicalType(facilityLoc, LocationPhysicalType.FACILITY));
        }

        return ret;
    }


    /**
     * generates a name for the Location resource, by combining the location name with all its parents
     */
    private static String generateName(BartsCsvHelper csvHelper, List<IdCellAndPhysicalType> locationHierarchy) throws Exception {
        List<String> tokens = new ArrayList<>();

        for (IdCellAndPhysicalType locationObj: locationHierarchy) {
            CsvCell cell = locationObj.getIdCell();

            CernerCodeValueRef codeRef = csvHelper.lookupCodeRef(CodeValueSet.LOCATION_NAME, cell);
            if (codeRef != null) {
                String codeDesc = codeRef.getCodeDispTxt();
                if (!Strings.isNullOrEmpty(codeDesc)) {
                    tokens.add(codeDesc);
                }
            }
        }

        return String.join(", ", tokens);
    }


    static class IdCellAndPhysicalType {
        private CsvCell idCell = null;
        private LocationPhysicalType physicalType = null;

        public IdCellAndPhysicalType(CsvCell idCell, LocationPhysicalType physicalType) {
            this.idCell = idCell;
            this.physicalType = physicalType;
        }

        public CsvCell getIdCell() {
            return idCell;
        }

        public LocationPhysicalType getPhysicalType() {
            return physicalType;
        }
    }
}
