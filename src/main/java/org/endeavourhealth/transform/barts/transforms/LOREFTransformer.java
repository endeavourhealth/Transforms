package org.endeavourhealth.transform.barts.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.AddressConverter;
import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.LocationPhysicalType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherTransform.InternalIdDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
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
    private static InternalIdDalI internalIdDAL = null;
    private static SimpleDateFormat formatDaily = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
    private static SimpleDateFormat formatBulk = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");
    private String uniqueId;

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

        String parentLocationResourceId = null;
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

        // Location resource id
        CsvCell locationIdCell = parser.getLocationId();
        ResourceId locationResourceId = getLocationResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, locationIdCell.getString());
        if (locationResourceId == null) {
            locationResourceId = new ResourceId();
            locationResourceId.setScopeId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE);
            locationResourceId.setResourceType("Location");
            locationResourceId.setUniqueId("LocationId=" + locationIdCell.getString());

            // Check if this location has previously been saved as a parent-location - Try secondary key
            String uniqueId = createSecondaryKey(facilityLoc, buildingLoc, surgeryLocationCode, ambulatoryLoc, nurseUnitLoc, roomLoc, bedLoc);
            String alternateResourceId = internalIdDAL.getDestinationId(fhirResourceFiler.getServiceId(), InternalIdMap.TYPE_ALTKEY_LOCATION, uniqueId);
            // Create main resource key
            if (alternateResourceId == null) {
                locationResourceId.setResourceId(UUID.randomUUID());

                // Create alternate keys for current location and all parents
                while (uniqueId != null) {
                    try {
                        //LOG.debug("Saving altkey:" + uniqueId);
                        internalIdDAL.insertRecord(fhirResourceFiler.getServiceId(), InternalIdMap.TYPE_ALTKEY_LOCATION, uniqueId, UUID.randomUUID().toString());
                    }
                    catch (Exception ex) {
                        // ignore duplicates
                    }
                    uniqueId = createParentKey(uniqueId);
                }
            } else {
                //LOG.debug("Found resource id " + alternateResourceId + " using altkey:" + uniqueId);
                locationResourceId.setResourceId(UUID.fromString(alternateResourceId));
                // Alternate keys for all parents should already exist
            }
            saveResourceId(locationResourceId);
        }

        // Get parent resource id using alternate key
        String uniqueId = createSecondaryKey(facilityLoc, buildingLoc, surgeryLocationCode, ambulatoryLoc, nurseUnitLoc, roomLoc, bedLoc);
        String parentUniqueId = createParentKey(uniqueId);
        //LOG.debug("Looking for parent location using key:" + parentUniqueId);
        if (parentUniqueId != null) {
            parentLocationResourceId = internalIdDAL.getDestinationId(fhirResourceFiler.getServiceId(), InternalIdMap.TYPE_ALTKEY_LOCATION, parentUniqueId);
        }

        // Organisation
        Address fhirOrgAddress = AddressConverter.createAddress(Address.AddressUse.WORK, "The Royal London Hospital", "Whitechapel", "London", "", "", "E1 1BB");
        ResourceId organisationResourceId = resolveOrganisationResource(parser.getCurrentState(), primaryOrgOdsCode, fhirResourceFiler, "Barts Health NHS Trust", fhirOrgAddress);

        // Create Location resource
        LocationBuilder locationBuilder = new LocationBuilder();

        //fhirLocation.setId(locationResourceId.getResourceId().toString());
        locationBuilder.setId(locationResourceId.getResourceId().toString(), locationIdCell);

        // Identifier
        //fhirLocation.addIdentifier().setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_LOCATION_ID).setValue(parser.getLocationId());
        if (!locationIdCell.isEmpty()) {
            IdentifierBuilder identifierBuilder = new IdentifierBuilder(locationBuilder);
            identifierBuilder.setUse(Identifier.IdentifierUse.SECONDARY);
            identifierBuilder.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_BARTS_LOCATION_ID);
            identifierBuilder.setValue(locationIdCell.getString(), locationIdCell);
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
        if (!bedLoc.isEmpty()) {
            //physicalType.addCoding().setCode(LocationPhysicalType.BD.getDisplay()).setSystem(LocationPhysicalType.BD.getSystem()).setDisplay(LocationPhysicalType.BD.getDefinition());
            //locationBuilder.setPhysicalType(physicalType, bedLoc);
            locationBuilder.setPhysicalType(LocationPhysicalType.BED, bedLoc);
        } else if (!roomLoc.isEmpty()) {
            //physicalType.addCoding().setCode(LocationPhysicalType.RO.getDisplay()).setSystem(LocationPhysicalType.RO.getSystem()).setDisplay(LocationPhysicalType.RO.getDefinition());
            //locationBuilder.setPhysicalType(physicalType, roomLoc);
            locationBuilder.setPhysicalType(LocationPhysicalType.ROOM, roomLoc);
        } else if (!nurseUnitLoc.isEmpty()) {
            //physicalType.addCoding().setCode(LocationPhysicalType.NULL.getDisplay()).setSystem(LocationPhysicalType.NULL.getSystem()).setDisplay(LocationPhysicalType.NULL.getDefinition());
            //locationBuilder.setPhysicalType(physicalType,nurseUnitLoc);
            locationBuilder.setPhysicalType(LocationPhysicalType.NURSEUNIT, nurseUnitLoc);
        } else if (!ambulatoryLoc.isEmpty()) {
            //physicalType.addCoding().setCode(LocationPhysicalType.NULL.getDisplay()).setSystem(LocationPhysicalType.NULL.getSystem()).setDisplay(LocationPhysicalType.NULL.getDefinition());
            //locationBuilder.setPhysicalType(physicalType, ambulatoryLoc);
            locationBuilder.setPhysicalType(LocationPhysicalType.AMBULATORY, ambulatoryLoc);
        } else if (!buildingLoc.isEmpty()) {
            //physicalType.addCoding().setCode(LocationPhysicalType.BU.getDisplay()).setSystem(LocationPhysicalType.BU.getSystem()).setDisplay(LocationPhysicalType.BU.getDefinition());
            //locationBuilder.setPhysicalType(physicalType, buildingLoc);
            locationBuilder.setPhysicalType(LocationPhysicalType.BUILDING, buildingLoc);
        } else if (!facilityLoc.isEmpty()) {
            //physicalType.addCoding().setCode(LocationPhysicalType.NULL.getDisplay()).setSystem(LocationPhysicalType.BU.getSystem()).setDisplay(LocationPhysicalType.BU.getDefinition());
            //locationBuilder.setPhysicalType(physicalType, facilityLoc);
            locationBuilder.setPhysicalType(LocationPhysicalType.FACILITY, facilityLoc);
        }

        // Mode
        //TODO complete
        //fhirLocation.setMode(Location.LocationMode.INSTANCE);
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
        /*dependencyList = new ArrayList<CsvCell>();
        String name = getName(fhirResourceFiler, parser.getFacilityLocation(),parser.getBuildingLocation(),parser.getAmbulatoryLocation(),parser.getNurseUnitLocation(),parser.getRoomLocation(),parser.getBedLcoation(), dependencyList);
        //fhirLocation.setName(name);
        dependencyArray = dependencyList.toArray(new CsvCell[dependencyList.size()]);
        locationBuilder.setName(name, dependencyArray);*/

        // managing org
        //TODO complete
        //fhirLocation.setManagingOrganization(ReferenceHelper.createReference(ResourceType.Organization, organisationResourceId.getResourceId().toString()));
        locationBuilder.setManagingOrganisation(ReferenceHelper.createReference(ResourceType.Organization, organisationResourceId.getResourceId().toString()));

        // Parent location
        if (parentLocationResourceId != null) {
            //fhirLocation.setPartOf(ReferenceHelper.createReference(ResourceType.Location, parentLocationResourceId));
            //TODO complete
            locationBuilder.setPartOf(ReferenceHelper.createReference(ResourceType.Location, parentLocationResourceId));
        }

        //LOG.debug("Save Location (LocationId=" + parser.getLocationId().getString() + "):" + FhirSerializationHelper.serializeResource(locationBuilder.getResource()));
        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), locationBuilder);
        //saveAdminResource(fhirResourceFiler, parser.getCurrentState(), locationBuilder);
    }

    private static String generateName(BartsCsvHelper csvHelper, CsvCell... sourceCells) throws Exception {
        List<String> tokens = new ArrayList<>();

        for (CsvCell cell: sourceCells) {
            if (!cell.isEmpty() && cell.getLong() > 0) {

                CernerCodeValueRef cernerCodeDef = csvHelper.lookUpCernerCodeFromCodeSet(CernerCodeValueRef.LOCATION_NAME, cell.getLong());
                String name = cernerCodeDef.getCodeDispTxt();
                if (!Strings.isNullOrEmpty(name)) {
                    tokens.add(name);
                } else {
                    tokens.add("Unknown Location (" + cell.getLong() + ")");
                }
            }
        }

        return String.join(", ", tokens);
    }


    private static String createSecondaryKey(CsvCell facilityCode, CsvCell buildingCode, CsvCell surgeryLocationCode, CsvCell ambulatoryCode, CsvCell nurseUnitCode, CsvCell roomCode, CsvCell bedCode) {

        List<String> tokens = new ArrayList<>();

        tokens.add("FacilityLocCode=" + facilityCode.getLong());

        if (!buildingCode.isEmpty() && buildingCode.getLong() > 0) {
            tokens.add("BuildingLocCode=" + buildingCode.getLong());
        }

        if (!surgeryLocationCode.isEmpty() && surgeryLocationCode.getLong() > 0) {
            tokens.add("SurgeryLocCode=" + surgeryLocationCode.getLong());
        }

        if (!ambulatoryCode.isEmpty() && ambulatoryCode.getLong() > 0) {
            tokens.add("AmbulatoryLocCode=" + ambulatoryCode.getLong());
        }

        if (!nurseUnitCode.isEmpty() && nurseUnitCode.getLong() > 0) {
            tokens.add("NurseUnitLocCode=" + nurseUnitCode.getLong());
        }

        if (!roomCode.isEmpty() && roomCode.getLong() > 0) {
            tokens.add("RoomLocCode=" + roomCode.getLong());
        }

        if (!bedCode.isEmpty() && bedCode.getLong() > 0) {
            tokens.add("BedLocCode=" + bedCode.getLong());
        }

        return String.join("-", tokens);
    }

    /*private static String createSecondaryKey(String facilityCode, String buildingCode, String ambulatoryCode, String nurseUnitCode, String roomCode, String bedCode) {
        StringBuffer sb = new StringBuffer();
        sb.append("FacilityLocCode=");
        sb.append(facilityCode);

        if (buildingCode != null && buildingCode.length() > 0 && buildingCode.compareTo("0") != 0) {
            sb.append("-");
            sb.append("BuildingLocCode=");
            sb.append(buildingCode);
        }

        if (ambulatoryCode != null && ambulatoryCode.length() > 0 && ambulatoryCode.compareTo("0") != 0) {
            sb.append("-");
            sb.append("AmbulatoryLocCode=");
            sb.append(ambulatoryCode);
        }

        if (nurseUnitCode != null && nurseUnitCode.length() > 0 && nurseUnitCode.compareTo("0") != 0) {
            sb.append("-");
            sb.append("NurseUnitLocCode=");
            sb.append(nurseUnitCode);
        }

        if (roomCode != null && roomCode.length() > 0 && roomCode.compareTo("0") != 0) {
            sb.append("-");
            sb.append("RoomLocCode=");
            sb.append(roomCode);
        }

        if (bedCode != null && bedCode.length() > 0 && bedCode.compareTo("0") != 0) {
            sb.append("-");
            sb.append("BedLocCode=");
            sb.append(bedCode);
        }

        return sb.toString();
    }*/

    /*
     *
     */
    private static String createParentKey(String uniqueKey) {
        // remove current level
        int lastpos = uniqueKey.lastIndexOf("-");
        if (lastpos == -1) {
            return null;
        } else {
            return uniqueKey.substring(0, lastpos);
        }
    }

    /*
     *
     */
    /*private static String createDescription(FhirResourceFilerI fhirResourceFiler, CsvCell facilityCode, CsvCell buildingCode, CsvCell ambulatoryCode, CsvCell nurseUnitCode, CsvCell roomCode, CsvCell bedCode, ArrayList<CsvCell> dependencyList) throws Exception {
        StringBuffer sb = new StringBuffer();
        if (!bedCode.isEmpty() && bedCode.getString().compareTo("0") != 0) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(getCodeRefValue(fhirResourceFiler, bedCode.getString()));
            dependencyList.add(bedCode);
        }

        if (!roomCode.isEmpty() && roomCode.getString().compareTo("0") != 0) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(getCodeRefValue(fhirResourceFiler, roomCode.getString()));
            dependencyList.add(roomCode);
        }

        if (!nurseUnitCode.isEmpty() && nurseUnitCode.getString().compareTo("0") != 0) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(getCodeRefValue(fhirResourceFiler, nurseUnitCode.getString()));
            dependencyList.add(nurseUnitCode);
        }

        if (!ambulatoryCode.isEmpty() && ambulatoryCode.getString().compareTo("0") != 0) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(getCodeRefValue(fhirResourceFiler, ambulatoryCode.getString()));
            dependencyList.add(ambulatoryCode);
        }

        if (!buildingCode.isEmpty() && buildingCode.getString().compareTo("0") != 0) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(getCodeRefValue(fhirResourceFiler, buildingCode.getString()));
            dependencyList.add(buildingCode);
        }

        if (sb.length() > 0) {
            sb.append(", ");
        }
        sb.append(getCodeRefValue(fhirResourceFiler, facilityCode.getString()));
        dependencyList.add(facilityCode);

        return sb.toString();
    }*/

    /*
     *
     */
    /*private static String getName(FhirResourceFilerI fhirResourceFiler, CsvCell facilityCode, CsvCell buildingCode, CsvCell ambulatoryCode, CsvCell nurseUnitCode, CsvCell roomCode, CsvCell bedCode, ArrayList<CsvCell> dependencyList) throws Exception {
        if (!bedCode.isEmpty() && bedCode.getString().compareTo("0") != 0) {
            dependencyList.add(bedCode);
            return getCodeRefValue(fhirResourceFiler, bedCode.getString());
        } else if (!roomCode.isEmpty() && roomCode.getString().compareTo("0") != 0) {
            dependencyList.add(roomCode);
            return getCodeRefValue(fhirResourceFiler, roomCode.getString());
        } else if (!nurseUnitCode.isEmpty() && nurseUnitCode.getString().compareTo("0") != 0) {
            dependencyList.add(nurseUnitCode);
            return getCodeRefValue(fhirResourceFiler, nurseUnitCode.getString());
        } else if (!ambulatoryCode.isEmpty() && ambulatoryCode.getString().compareTo("0") != 0) {
            dependencyList.add(ambulatoryCode);
            return getCodeRefValue(fhirResourceFiler, ambulatoryCode.getString());
        } else if (!buildingCode.isEmpty() && buildingCode.getString().compareTo("0") != 0) {
            dependencyList.add(buildingCode);
            return getCodeRefValue(fhirResourceFiler, buildingCode.getString());
        } else {
            dependencyList.add(facilityCode);
            return getCodeRefValue(fhirResourceFiler, facilityCode.getString());
        }
    }*/

    /*
     *
     */
    /*
    private static CodeableConcept getPhysicalType(String facilityCode, String buildingCode, String ambulatoryCode, String nurseUnitCode, String roomCode, String bedCode) {
        CodeableConcept ret = null;
        if (bedCode != null && bedCode.length() > 0) {
            ret.addCoding().setCode(LocationPhysicalType.BD.getDefinition()).setSystem(LocationPhysicalType.BD.getSystem()).setDisplay(LocationPhysicalType.BD.getDisplay());
            return ret;
        } else if (roomCode != null && roomCode.length() > 0) {
            ret.addCoding().setCode(LocationPhysicalType.RO.getDefinition()).setSystem(LocationPhysicalType.RO.getSystem()).setDisplay(LocationPhysicalType.RO.getDisplay());
            return ret;
        } else if (nurseUnitCode != null && nurseUnitCode.length() > 0) {
            ret.addCoding().setCode(LocationPhysicalType.NULL.getDefinition()).setSystem(LocationPhysicalType.NULL.getSystem()).setDisplay(LocationPhysicalType.NULL.getDisplay());
            return ret;
        } else if (ambulatoryCode != null && ambulatoryCode.length() > 0) {
            ret.addCoding().setCode(LocationPhysicalType.NULL.getDefinition()).setSystem(LocationPhysicalType.NULL.getSystem()).setDisplay(LocationPhysicalType.NULL.getDisplay());
            return ret;
        } else if (buildingCode != null && buildingCode.length() > 0) {
            ret.addCoding().setCode(LocationPhysicalType.BU.getDefinition()).setSystem(LocationPhysicalType.BU.getSystem()).setDisplay(LocationPhysicalType.BU.getDisplay());
            return ret;
        } else {
            ret.addCoding().setCode(LocationPhysicalType.NULL.getDefinition()).setSystem(LocationPhysicalType.NULL.getSystem()).setDisplay(LocationPhysicalType.NULL.getDisplay());
            return ret;
        }
    } */

    /*
     *
     */
    /*private static String getCodeRefValue(FhirResourceFilerI fhirResourceFiler, String code) throws Exception {
        CernerCodeValueRef cernerCodeDef = cernerCodeValueRefDAL.getCodeFromCodeSet(CernerCodeValueRef.LOCATION_NAME, Long.valueOf(code), fhirResourceFiler.getServiceId());
        if (cernerCodeDef != null) {
            return cernerCodeDef.getCodeDispTxt();
        } else {
            String ret = "??Unknown location " + code;
            // LOG.warn("Code not found in Code Value lookup:" + ret);
            return ret;
        }
    }*/
}
