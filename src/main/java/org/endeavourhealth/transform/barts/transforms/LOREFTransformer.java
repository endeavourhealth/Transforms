package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.AddressHelper;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.LocationPhysicalType;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.CodeValueSet;
import org.endeavourhealth.transform.barts.cache.LocationResourceCache;
import org.endeavourhealth.transform.barts.schema.LOREF;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.FhirResourceFilerI;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.LocationBuilder;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

public class LOREFTransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(LOREFTransformer.class);
    private static SimpleDateFormat formatDaily = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
    private static SimpleDateFormat formatBulk = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");

    /*
     *
     */
    public static void transform(String version,
                                 List<ParserI> parsers,
                                 FhirResourceFilerI fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        for (ParserI parser: parsers) {
            while (parser.nextRecord()) {
                try {
                    createLocation((LOREF) parser, (FhirResourceFiler) fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);

                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }
    }


    /*
     *
     */
    public static void createLocation(LOREF parser,
                                      FhirResourceFiler fhirResourceFiler,
                                      BartsCsvHelper csvHelper,
                                      String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {

        //LOG.debug("Line number " + parser.getCurrentLineNumber() + " locationId " +  parser.getLocationId().getString());

        ResourceId parentLocationResourceId = null;
        // Extract locations
        CsvCell facilityLoc = parser.getFacilityLocation();
        CsvCell buildingLoc = parser.getBuildingLocation();
        CsvCell surgeryLocationCode = parser.getSurgeryLocation();
        CsvCell ambulatoryLoc = parser.getAmbulatoryLocation();
        CsvCell nurseUnitLoc = parser.getNurseUnitLocation();
        CsvCell roomLoc = parser.getRoomLocation();
        CsvCell bedLoc = parser.getBedLcoation();
        CsvCell beginDateCell = parser.getBeginEffectiveDateTime();
        CsvCell locationIdCell = parser.getLocationId();

        Date beginDate = null;
        try {
            beginDate = formatDaily.parse(beginDateCell.getString());
        } catch (ParseException ex) {
            beginDate = formatBulk.parse(beginDateCell.getString());
        }
        CsvCell endDateCell = parser.getEndEffectiveDateTime();
        Date endDate = null;
        try {
            endDate = formatDaily.parse(endDateCell.getString());
        } catch (ParseException ex) {
            endDate = formatBulk.parse(endDateCell.getString());
        }
        //LOG.debug("Location active from " + beginDate.toString() + " until " + endDate.toString());

        // Location resource id
        LocationBuilder locationBuilder = LocationResourceCache.getLocationBuilder(csvHelper, locationIdCell);
        if (locationBuilder == null) {
            locationBuilder = LocationResourceCache.createLocationBuilder(locationIdCell);
        }

        createMissingReferencedLocations(facilityLoc, buildingLoc, surgeryLocationCode, ambulatoryLoc, nurseUnitLoc, roomLoc, bedLoc, fhirResourceFiler, parser, csvHelper);

        // Get parent resource id
        String parentId = getParentId(facilityLoc, buildingLoc, surgeryLocationCode, ambulatoryLoc, nurseUnitLoc, roomLoc, bedLoc);
        if (parentId != null) {
            parentLocationResourceId = getLocationResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parentId);
        }

        // Organisation
        Address fhirOrgAddress = AddressHelper.createAddress(Address.AddressUse.WORK, "The Royal London Hospital", "Whitechapel", "London", "", "", "E1 1BB");
        ResourceId organisationResourceId = resolveOrganisationResource(parser.getCurrentState(), primaryOrgOdsCode, fhirResourceFiler, "Barts Health NHS Trust", fhirOrgAddress);

        // Identifier
        if (!locationIdCell.isEmpty()) {
            List<Identifier> identifiers = IdentifierBuilder.findExistingIdentifiersForSystem(locationBuilder, FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_LOCATION_ID);
            if (identifiers.size() == 0) {
                IdentifierBuilder identifierBuilder = new IdentifierBuilder(locationBuilder);
                identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_LOCATION_ID);
                identifierBuilder.setUse(Identifier.IdentifierUse.OFFICIAL);
                identifierBuilder.setValue(locationIdCell.getString(), locationIdCell);
            }
        }

        // Status
        Date now = new Date();
        if (now.after(beginDate) && now.before(endDate)) {
            //fhirLocation.setStatus(Location.LocationStatus.ACTIVE);
            locationBuilder.setStatus(Location.LocationStatus.ACTIVE, beginDateCell, endDateCell);
        } else {
            //fhirLocation.setStatus(Location.LocationStatus.INACTIVE);
            locationBuilder.setStatus(Location.LocationStatus.INACTIVE, beginDateCell, endDateCell);
        }

        // Physical type
        //fhirLocation.setPhysicalType(getPhysicalType(facilityLoc.getString(),buildingLoc.getString(),ambulatoryLoc.getString(),nurseUnitLoc.getString(),roomLoc .getString(),bedLoc.getString()));
        CodeableConcept physicalType = new CodeableConcept();
        if (!bedLoc.isEmpty() && bedLoc.getLong() > 0) {
            locationBuilder.setPhysicalType(LocationPhysicalType.BED, bedLoc);
        } else if (!roomLoc.isEmpty() && roomLoc.getLong() > 0) {
            locationBuilder.setPhysicalType(LocationPhysicalType.ROOM, roomLoc);
        } else if (!nurseUnitLoc.isEmpty() && nurseUnitLoc.getLong() > 0) {
            locationBuilder.setPhysicalType(LocationPhysicalType.NURSEUNIT, nurseUnitLoc);
        } else if (!ambulatoryLoc.isEmpty() && ambulatoryLoc.getLong() > 0) {
            locationBuilder.setPhysicalType(LocationPhysicalType.AMBULATORY, ambulatoryLoc);
        } else if (!buildingLoc.isEmpty() && buildingLoc.getLong() > 0) {
            locationBuilder.setPhysicalType(LocationPhysicalType.BUILDING, buildingLoc);
        } else if (!facilityLoc.isEmpty() && facilityLoc.getLong() > 0) {
            locationBuilder.setPhysicalType(LocationPhysicalType.FACILITY, facilityLoc);
        }

        // Mode
        locationBuilder.setMode(Location.LocationMode.INSTANCE);

        // Description
        /*ArrayList<CsvCell> dependencyList = new ArrayList<CsvCell>();
        String description = createDescription(fhirResourceFiler, facilityLoc,buildingLoc,ambulatoryLoc,nurseUnitLoc,roomLoc,bedLoc, dependencyList);
        //fhirLocation.setDescription(description);
        CsvCell[] dependencyArray = dependencyList.toArray(new CsvCell[dependencyList.size()]);
        locationBuilder.setDescription(description, dependencyArray);*/

        // Name
        String name = generateName(csvHelper, facilityLoc, buildingLoc, surgeryLocationCode, ambulatoryLoc, nurseUnitLoc, roomLoc, bedLoc);
        locationBuilder.setName(name, facilityLoc, buildingLoc, surgeryLocationCode, ambulatoryLoc, nurseUnitLoc, roomLoc, bedLoc);

        // managing org
        locationBuilder.setManagingOrganisation(ReferenceHelper.createReference(ResourceType.Organization, organisationResourceId.getResourceId().toString()));

        // Parent location
        if (parentLocationResourceId != null) {
            locationBuilder.setPartOf(ReferenceHelper.createReference(ResourceType.Location, parentLocationResourceId.getResourceId().toString()));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Save Location (LocationId=" + parser.getLocationId().getString() + "):" + FhirSerializationHelper.serializeResource(locationBuilder.getResource()));
        }
    }

    /*
     *
     */
    private static void createMissingReferencedLocations(CsvCell facilityCode, CsvCell buildingCode, CsvCell surgeryLocationCode, CsvCell ambulatoryCode, CsvCell nurseUnitCode, CsvCell roomCode, CsvCell bedCode, FhirResourceFiler fhirResourceFiler, ParserI parser, BartsCsvHelper csvHelper) throws Exception {
        ResourceId locationResourceId = null;
        UUID uuid = null;
        LocationBuilder subLocationBuilder;

        if (!facilityCode.isEmpty() && facilityCode.getLong() > 0) {
            uuid = LocationResourceCache.getOrCreateLocationUUID(csvHelper, facilityCode);
        }

        if (!buildingCode.isEmpty() && buildingCode.getLong() > 0) {
            uuid = LocationResourceCache.getOrCreateLocationUUID(csvHelper, buildingCode);
        }

        if (!surgeryLocationCode.isEmpty() && surgeryLocationCode.getLong() > 0) {
            uuid = LocationResourceCache.getOrCreateLocationUUID(csvHelper, surgeryLocationCode);
        }

        if (!ambulatoryCode.isEmpty() && ambulatoryCode.getLong() > 0) {
            uuid = LocationResourceCache.getOrCreateLocationUUID(csvHelper, ambulatoryCode);
        }

        if (!nurseUnitCode.isEmpty() && nurseUnitCode.getLong() > 0) {
            uuid = LocationResourceCache.getOrCreateLocationUUID(csvHelper, nurseUnitCode);
        }

        if (!roomCode.isEmpty() && roomCode.getLong() > 0) {
            uuid = LocationResourceCache.getOrCreateLocationUUID(csvHelper, roomCode);
        }

        if (!bedCode.isEmpty() && bedCode.getLong() > 0) {
            uuid = LocationResourceCache.getOrCreateLocationUUID(csvHelper, bedCode);
        }
    }

    /*
     *
     */
    private static String generateName(BartsCsvHelper csvHelper, CsvCell... sourceCells) throws Exception {
        List<String> tokens = new ArrayList<>();

        for (CsvCell cell: sourceCells) {
            if ((cell != null) && (!cell.isEmpty()) && (cell.getLong() > 0)) {

                CernerCodeValueRef cernerCodeDef = csvHelper.lookupCodeRef(CodeValueSet.LOCATION_NAME, cell);
                if (cernerCodeDef !=null && cernerCodeDef.getCodeDispTxt() != null) {
                    tokens.add(cernerCodeDef.getCodeDispTxt());
                } else {
                    tokens.add("Unknown Location (" + cell.getLong() + ")");
                }
            }
        }

        return String.join(", ", tokens);
    }

    private static String getParentId(CsvCell facilityCode, CsvCell buildingCode, CsvCell surgeryLocationCode, CsvCell ambulatoryCode, CsvCell nurseUnitCode, CsvCell roomCode, CsvCell bedCode) {
        ArrayList<String> locationList = new ArrayList<String>();

        if (!bedCode.isEmpty() && bedCode.getLong() > 0) {
            locationList.add(bedCode.getString());
        }

        if (!roomCode.isEmpty() && roomCode.getLong() > 0) {
            locationList.add(roomCode.getString());
        }

        if (!nurseUnitCode.isEmpty() && nurseUnitCode.getLong() > 0) {
            locationList.add(nurseUnitCode.getString());
        }

        if (!ambulatoryCode.isEmpty() && ambulatoryCode.getLong() > 0) {
            locationList.add(ambulatoryCode.getString());
        }

        if (!surgeryLocationCode.isEmpty() && surgeryLocationCode.getLong() > 0) {
            locationList.add(surgeryLocationCode.getString());
        }

        if (!buildingCode.isEmpty() && buildingCode.getLong() > 0) {
            locationList.add(buildingCode.getString());
        }

        if (!facilityCode.isEmpty() && facilityCode.getLong() > 0) {
            locationList.add(facilityCode.getString());
        }

        if (locationList.size() > 1) {
            return locationList.get(1);
        } else {
            return null;
        }
    }

}
