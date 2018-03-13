package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.AddressConverter;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.LocationPhysicalType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherTransform.InternalIdDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.schema.LOREF;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.FhirResourceFilerI;
import org.endeavourhealth.transform.common.ParserI;
import org.endeavourhealth.transform.common.TransformWarnings;
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

public class LOREFTransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(LOREFTransformer.class);
    private static InternalIdDalI internalIdDAL = null;
    private static SimpleDateFormat formatDaily = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
    private static SimpleDateFormat formatBulk = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");

    /*
     *
     */
    public static void transform(String version,
                                 ParserI parser,
                                 FhirResourceFilerI fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        if (parser == null) {
            return;
        }

        while (parser.nextRecord()) {
            try {
                createLocation((LOREF)parser, (FhirResourceFiler) fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);

            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
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

        if (internalIdDAL == null) {
            internalIdDAL = DalProvider.factoryInternalIdDal();
        }

        LocationBuilder locationBuilder = null;
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
        CsvCell locationIdCell = parser.getLocationId();
        ResourceId locationResourceId = getLocationResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, locationIdCell.getString());
        if (locationResourceId == null) {
            // New location id - check if other location has referenced this as a parent
            locationResourceId = createLocationResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, locationIdCell.getString());

            locationBuilder = new LocationBuilder();
            locationBuilder.setId(locationResourceId.getResourceId().toString(), locationIdCell);
        } else {
            Location location = (Location) csvHelper.retrieveResource(ResourceType.Location, locationResourceId.getResourceId());
            if (location != null) {
                locationBuilder = new LocationBuilder(location);
            } else {
                locationBuilder = new LocationBuilder();
                locationBuilder.setId(locationResourceId.getResourceId().toString(), locationIdCell);
            }
        }

        createMissingReferencedLocations(facilityLoc, buildingLoc, surgeryLocationCode, ambulatoryLoc, nurseUnitLoc, roomLoc, bedLoc, fhirResourceFiler, parser, csvHelper);

        // Get parent resource id
        String parentId = getParentId(facilityLoc, buildingLoc, surgeryLocationCode, ambulatoryLoc, nurseUnitLoc, roomLoc, bedLoc);
        if (parentId != null) {
            parentLocationResourceId = getLocationResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parentId);
        }

        // Organisation
        Address fhirOrgAddress = AddressConverter.createAddress(Address.AddressUse.WORK, "The Royal London Hospital", "Whitechapel", "London", "", "", "E1 1BB");
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
        saveAdminResource(fhirResourceFiler, parser.getCurrentState(), locationBuilder);
    }

    /*
     *
     */
    private static void createMissingReferencedLocations(CsvCell facilityCode, CsvCell buildingCode, CsvCell surgeryLocationCode, CsvCell ambulatoryCode, CsvCell nurseUnitCode, CsvCell roomCode, CsvCell bedCode, FhirResourceFiler fhirResourceFiler, ParserI parser, BartsCsvHelper csvHelper) throws Exception {
        ResourceId locationResourceId = null;

        if (!facilityCode.isEmpty() && facilityCode.getLong() > 0) {
            locationResourceId = getLocationResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, facilityCode.getString());
            if (locationResourceId == null) {
                createPlaceholderLocation(facilityCode.getString(), fhirResourceFiler, parser, csvHelper);
            }
        }

        if (!buildingCode.isEmpty() && buildingCode.getLong() > 0) {
            locationResourceId = getLocationResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, buildingCode.getString());
            if (locationResourceId == null) {
                createPlaceholderLocation(buildingCode.getString(), fhirResourceFiler, parser, csvHelper);
            }
        }

        if (!surgeryLocationCode.isEmpty() && surgeryLocationCode.getLong() > 0) {
            locationResourceId = getLocationResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, surgeryLocationCode.getString());
            if (locationResourceId == null) {
                createPlaceholderLocation(surgeryLocationCode.getString(), fhirResourceFiler, parser, csvHelper);
            }
        }

        if (!ambulatoryCode.isEmpty() && ambulatoryCode.getLong() > 0) {
            locationResourceId = getLocationResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, ambulatoryCode.getString());
            if (locationResourceId == null) {
                createPlaceholderLocation(ambulatoryCode.getString(), fhirResourceFiler, parser, csvHelper);
            }
        }

        if (!nurseUnitCode.isEmpty() && nurseUnitCode.getLong() > 0) {
            locationResourceId = getLocationResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, nurseUnitCode.getString());
            if (locationResourceId == null) {
                createPlaceholderLocation(nurseUnitCode.getString(), fhirResourceFiler, parser, csvHelper);
            }
        }

        if (!roomCode.isEmpty() && roomCode.getLong() > 0) {
            locationResourceId = getLocationResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, roomCode.getString());
            if (locationResourceId == null) {
                createPlaceholderLocation(roomCode.getString(), fhirResourceFiler, parser, csvHelper);
            }
        }

        if (!bedCode.isEmpty() && bedCode.getLong() > 0) {
            locationResourceId = getLocationResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, bedCode.getString());
            if (locationResourceId == null) {
                createPlaceholderLocation(bedCode.getString(), fhirResourceFiler, parser, csvHelper);
            }
        }
    }

    /*
     *
     */
    private static void createPlaceholderLocation(String locationId, FhirResourceFiler fhirResourceFiler, ParserI parser, BartsCsvHelper csvHelper) throws Exception {
        CernerCodeValueRef cernerCodeValueRef = csvHelper.lookUpCernerCodeFromCodeSet(CernerCodeValueRef.LOCATION_NAME, Long.valueOf(locationId));
        if (cernerCodeValueRef != null) {
            ResourceId resourceId = createLocationResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, locationId);

            LocationBuilder locationBuilder = new LocationBuilder();
            locationBuilder.setId(resourceId.getResourceId().toString());
            locationBuilder.setStatus(Location.LocationStatus.ACTIVE);
            locationBuilder.setMode(Location.LocationMode.INSTANCE);
            locationBuilder.setName(cernerCodeValueRef.getCodeDispTxt());
            saveAdminResource(fhirResourceFiler, parser.getCurrentState(), locationBuilder);
        } else {
            TransformWarnings.log(LOG, parser, "Location id not found in CVREF for Location-id {} in file {}", locationId, parser.getFilePath());
        }
    }

    /*
     *
     */
    private static String generateName(BartsCsvHelper csvHelper, CsvCell... sourceCells) throws Exception {
        List<String> tokens = new ArrayList<>();

        for (CsvCell cell: sourceCells) {
            if ((cell != null) && (!cell.isEmpty()) && (cell.getLong() > 0)) {

                CernerCodeValueRef cernerCodeDef = csvHelper.lookUpCernerCodeFromCodeSet(CernerCodeValueRef.LOCATION_NAME, cell.getLong());
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
        if (!roomCode.isEmpty() && roomCode.getLong() > 0) {
            return roomCode.getString();
        }

        if (!nurseUnitCode.isEmpty() && nurseUnitCode.getLong() > 0) {
            return nurseUnitCode.getString();
        }

        if (!ambulatoryCode.isEmpty() && ambulatoryCode.getLong() > 0) {
            return ambulatoryCode.getString();
        }

        if (!surgeryLocationCode.isEmpty() && surgeryLocationCode.getLong() > 0) {
            return surgeryLocationCode.getString();
        }

        if (!buildingCode.isEmpty() && buildingCode.getLong() > 0) {
            return buildingCode.getString();
        }

        if (!facilityCode.isEmpty() && facilityCode.getLong() > 0) {
            return facilityCode.getString();
        }

        return null;
    }

}
