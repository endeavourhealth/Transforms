package org.endeavourhealth.transform.barts.transforms;

import org.endeavourhealth.common.fhir.AddressConverter;
import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.hl7receiver.models.ResourceId;
import org.endeavourhealth.core.database.dal.publisherTransform.CernerCodeValueRefDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.InternalIdDalI;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.database.rdbms.publisherTransform.RdbmsInternalIdDal;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.barts.BartsCsvHelper;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.barts.schema.LOREF;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.FhirResourceFilerI;
import org.endeavourhealth.transform.common.resourceBuilders.LocationBuilder;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.UUID;

public class LOREFTransformer extends BartsBasisTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(LOREFTransformer.class);
    private static InternalIdDalI internalIdDAL = null;
    private static CernerCodeValueRefDalI cernerCodeValueRefDAL = null;
    private static SimpleDateFormat formatDaily = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
    private static SimpleDateFormat formatBulk = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");
    private String uniqueId;

    /*
     *
     */
    public static void transform(String version,
                                 LOREF parser,
                                 FhirResourceFilerI fhirResourceFiler,
                                 BartsCsvHelper csvHelper,
                                 String primaryOrgOdsCode,
                                 String primaryOrgHL7OrgOID) throws Exception {

        while (parser.nextRecord()) {
            try {
                String valStr = validateEntry(parser);
                if (valStr == null) {
                    createLocation(parser, (FhirResourceFiler) fhirResourceFiler, csvHelper, version, primaryOrgOdsCode, primaryOrgHL7OrgOID);
                } else {
                    LOG.debug("Validation error:" + valStr);
                    SlackHelper.sendSlackMessage(SlackHelper.Channel.QueueReaderAlerts, valStr);
                }
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    /*
     *
     */
    public static String validateEntry(LOREF parser) {
        return null;
    }


    /*
     *
     */
    public static void createLocation(LOREF parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       BartsCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {

        LOG.debug("Line number " + parser.getCurrentLineNumber() + " locationId " +  parser.getLocationId().getString());

        if (internalIdDAL == null) {
            internalIdDAL = DalProvider.factoryInternalIdDal();
        }

        if (cernerCodeValueRefDAL == null) {
            cernerCodeValueRefDAL = DalProvider.factoryCernerCodeValueRefDal();
        }

        String parentLocationResourceId = null;
        // Extract locations
        CsvCell facilityLoc = parser.getFacilityLocation();
        CsvCell buildingLoc = parser.getBuildingLocation();
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
            String uniqueId = createSecondaryKey(facilityLoc.getString(),buildingLoc.getString(),ambulatoryLoc.getString(),nurseUnitLoc.getString(),roomLoc .getString(),bedLoc.getString());
            String alternateResourceId = internalIdDAL.getDestinationId(fhirResourceFiler.getServiceId(), RdbmsInternalIdDal.IDTYPE_ALTKEY_LOCATION, uniqueId);
            // Create main resource key
            if (alternateResourceId == null) {
                locationResourceId.setResourceId(UUID.randomUUID());

                // Create alternate keys for current location and all parents
                while (uniqueId != null) {
                    try {
                        LOG.debug("Saving altkey:" + uniqueId);
                        internalIdDAL.insertRecord(fhirResourceFiler.getServiceId(), RdbmsInternalIdDal.IDTYPE_ALTKEY_LOCATION, uniqueId, UUID.randomUUID().toString());
                    }
                    catch (Exception ex) {
                        // ignore duplicates
                    }
                    uniqueId = createParentKey(uniqueId);
                }
            } else {
                LOG.debug("Found resource id " + alternateResourceId + " using altkey:" + uniqueId);
                locationResourceId.setResourceId(UUID.fromString(alternateResourceId));
                // Alternate keys for all parents should already exist
            }
            saveResourceId(locationResourceId);
        }

        // Get parent resource id using alternate key
        String uniqueId = createSecondaryKey(facilityLoc.getString(),buildingLoc.getString(),ambulatoryLoc.getString(),nurseUnitLoc.getString(),roomLoc .getString(),bedLoc.getString());
        String parentUniqueId = createParentKey(uniqueId);
        LOG.debug("Looking for parent location using key:" + parentUniqueId);
        if (parentUniqueId != null) {
            parentLocationResourceId = internalIdDAL.getDestinationId(fhirResourceFiler.getServiceId(), RdbmsInternalIdDal.IDTYPE_ALTKEY_LOCATION, parentUniqueId);
        }

        // Organisation
        Address fhirOrgAddress = AddressConverter.createAddress(Address.AddressUse.WORK, "The Royal London Hospital", "Whitechapel", "London", "", "", "E1 1BB");
        ResourceId organisationResourceId = resolveOrganisationResource(parser.getCurrentState(), primaryOrgOdsCode, fhirResourceFiler, "Barts Health NHS Trust", fhirOrgAddress);

        // Create Location resource
        LocationBuilder locationBuilder = new LocationBuilder();

        //fhirLocation.setId(locationResourceId.getResourceId().toString());
        locationBuilder.setId(locationResourceId.getResourceId().toString(), locationIdCell);

        // Identifier
        //fhirLocation.addIdentifier().setSystem(FhirUri.IDENTIFIER_SYSTEM_BARTS_LOCATION_ID).setValue(parser.getLocationId());
        Identifier fhirIdentifier = new Identifier();
        fhirIdentifier.setSystem(FhirUri.IDENTIFIER_SYSTEM_BARTS_LOCATION_ID).setValue(locationIdCell.getString());
        locationBuilder.addIdentifier(fhirIdentifier, locationIdCell);

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
            locationBuilder.setPhysicalType(CodeableConceptHelper.createCodeableConcept(org.endeavourhealth.common.fhir.schema.LocationPhysicalType.BED), bedLoc);
        } else if (!roomLoc.isEmpty()) {
            //physicalType.addCoding().setCode(LocationPhysicalType.RO.getDisplay()).setSystem(LocationPhysicalType.RO.getSystem()).setDisplay(LocationPhysicalType.RO.getDefinition());
            //locationBuilder.setPhysicalType(physicalType, roomLoc);
            locationBuilder.setPhysicalType(CodeableConceptHelper.createCodeableConcept(org.endeavourhealth.common.fhir.schema.LocationPhysicalType.ROOM), roomLoc);
        } else if (!nurseUnitLoc.isEmpty()) {
            //physicalType.addCoding().setCode(LocationPhysicalType.NULL.getDisplay()).setSystem(LocationPhysicalType.NULL.getSystem()).setDisplay(LocationPhysicalType.NULL.getDefinition());
            //locationBuilder.setPhysicalType(physicalType,nurseUnitLoc);
            locationBuilder.setPhysicalType(CodeableConceptHelper.createCodeableConcept(org.endeavourhealth.common.fhir.schema.LocationPhysicalType.NURSEUNIT), nurseUnitLoc);
        } else if (!ambulatoryLoc.isEmpty()) {
            //physicalType.addCoding().setCode(LocationPhysicalType.NULL.getDisplay()).setSystem(LocationPhysicalType.NULL.getSystem()).setDisplay(LocationPhysicalType.NULL.getDefinition());
            //locationBuilder.setPhysicalType(physicalType, ambulatoryLoc);
            locationBuilder.setPhysicalType(CodeableConceptHelper.createCodeableConcept(org.endeavourhealth.common.fhir.schema.LocationPhysicalType.AMBULATORY), ambulatoryLoc);
        } else if (!buildingLoc.isEmpty()) {
            //physicalType.addCoding().setCode(LocationPhysicalType.BU.getDisplay()).setSystem(LocationPhysicalType.BU.getSystem()).setDisplay(LocationPhysicalType.BU.getDefinition());
            //locationBuilder.setPhysicalType(physicalType, buildingLoc);
            locationBuilder.setPhysicalType(CodeableConceptHelper.createCodeableConcept(org.endeavourhealth.common.fhir.schema.LocationPhysicalType.BUILDING), buildingLoc);
        } else if (!facilityLoc.isEmpty()) {
            //physicalType.addCoding().setCode(LocationPhysicalType.NULL.getDisplay()).setSystem(LocationPhysicalType.BU.getSystem()).setDisplay(LocationPhysicalType.BU.getDefinition());
            //locationBuilder.setPhysicalType(physicalType, facilityLoc);
            locationBuilder.setPhysicalType(CodeableConceptHelper.createCodeableConcept(org.endeavourhealth.common.fhir.schema.LocationPhysicalType.FACILITY), facilityLoc);
        }

        // Mode
        //TODO complete
        //fhirLocation.setMode(Location.LocationMode.INSTANCE);
        locationBuilder.setMode(Location.LocationMode.INSTANCE);

        // Description
        ArrayList<CsvCell> dependencyList = new ArrayList<CsvCell>();
        String description = createDescription(fhirResourceFiler, facilityLoc,buildingLoc,ambulatoryLoc,nurseUnitLoc,roomLoc,bedLoc, dependencyList);
        //fhirLocation.setDescription(description);
        CsvCell[] dependencyArray = dependencyList.toArray(new CsvCell[dependencyList.size()]);
        locationBuilder.setDescription(description, dependencyArray);

        // Name
        dependencyList = new ArrayList<CsvCell>();
        String name = getName(fhirResourceFiler, parser.getFacilityLocation(),parser.getBuildingLocation(),parser.getAmbulatoryLocation(),parser.getNurseUnitLocation(),parser.getRoomLocation(),parser.getBedLcoation(), dependencyList);
        //fhirLocation.setName(name);
        dependencyArray = dependencyList.toArray(new CsvCell[dependencyList.size()]);
        locationBuilder.setName(name, dependencyArray);

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

        LOG.debug("Save Location (LocationId=" + parser.getLocationId().getString() + "):" + FhirSerializationHelper.serializeResource(locationBuilder.getResource()));
        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), locationBuilder);
        //saveAdminResource(fhirResourceFiler, parser.getCurrentState(), locationBuilder);
    }

    /*
    public static void createEncounter(LOREF parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       BartsCsvHelper csvHelper,
                                       String version, String primaryOrgOdsCode, String primaryOrgHL7OrgOID) throws Exception {

        String parentLocationResourceId = null;

        // Location resource id
        ResourceId locationResourceId = getLocationResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getLocationId());
        if (locationResourceId == null) {
            locationResourceId = createLocationResourceId(BartsCsvToFhirTransformer.BARTS_RESOURCE_ID_SCOPE, parser.getLocationId());

            if (internalIdDAL == null) {
                internalIdDAL = DalProvider.factoryInternalIdDal();
                cernerCodeValueRefDAL = DalProvider.factoryCernerCodeValueRefDal();
            }

            // Try secondary key
            String uniqueId = createSecondaryKey(parser.getFacilityLocation(),parser.getBuildingLocation(),parser.getAmbulatoryLocation(),parser.getNurseUnitLocation(),parser.getRoomLocation(),parser.getBedLcoation());
            String alternateResourceId = internalIdDAL.getDestinationId(fhirResourceFiler.getServiceId(), RdbmsInternalIdDal.IDTYPE_ALTKEY_LOCATION, uniqueId);
            // Create main resource key
            if (alternateResourceId == null) {
                locationResourceId.setResourceId(UUID.randomUUID());

                // Create alternate keys for all parents (if missing)
                String parentUniqueId = createParentKey(uniqueId);
                while (parentUniqueId != null) {
                    try {
                        internalIdDAL.insertRecord(fhirResourceFiler.getServiceId(), RdbmsInternalIdDal.IDTYPE_ALTKEY_LOCATION, parentUniqueId, UUID.randomUUID().toString());
                    }
                    catch (Exception ex) {
                        // ignore duplicates
                    }
                    parentUniqueId = createParentKey(parentUniqueId);
                }
            } else {
                locationResourceId.setResourceId(UUID.fromString(alternateResourceId));
                // Alternate keys for all parents should already exist
            }
            saveResourceId(locationResourceId);
        }

        // Get parent resoruce id using alternate key
        String uniqueId = createSecondaryKey(parser.getFacilityLocation(),parser.getBuildingLocation(),parser.getAmbulatoryLocation(),parser.getNurseUnitLocation(),parser.getRoomLocation(),parser.getBedLcoation());
        String parentUniqueId = createParentKey(uniqueId);
        parentLocationResourceId = internalIdDAL.getDestinationId(fhirResourceFiler.getServiceId(), RdbmsInternalIdDal.IDTYPE_ALTKEY_LOCATION, parentUniqueId);

        // Organisation
        Address fhirOrgAddress = AddressConverter.createAddress(Address.AddressUse.WORK, "The Royal London Hospital", "Whitechapel", "London", "", "", "E1 1BB");
        ResourceId organisationResourceId = resolveOrganisationResource(parser.getCurrentState(), primaryOrgOdsCode, fhirResourceFiler, "Barts Health NHS Trust", fhirOrgAddress);

        // Create Location resource
        Location fhirLocation = new Location();
        fhirLocation.setId(locationResourceId.getResourceId().toString());

        fhirLocation.addIdentifier().setSystem(FhirUri.IDENTIFIER_SYSTEM_BARTS_LOCATION_ID).setValue(parser.getLocationId());

        // Status
        Date now = new Date();
        if (now.after(parser.getBeginEffectiveDateTime()) && now.before(parser.getEndEffectiveDateTime())) {
            fhirLocation.setStatus(Location.LocationStatus.ACTIVE);
        } else {
            fhirLocation.setStatus(Location.LocationStatus.INACTIVE);
        }

        // Physical type
        fhirLocation.setPhysicalType(getPhysicalType(parser.getFacilityLocation(),parser.getBuildingLocation(),parser.getAmbulatoryLocation(),parser.getNurseUnitLocation(),parser.getRoomLocation(),parser.getBedLcoation()));

        fhirLocation.setMode(Location.LocationMode.INSTANCE);

        fhirLocation.setDescription(createDescription(fhirResourceFiler, parser.getFacilityLocation(),parser.getBuildingLocation(),parser.getAmbulatoryLocation(),parser.getNurseUnitLocation(),parser.getRoomLocation(),parser.getBedLcoation()));

        fhirLocation.setName(getName(fhirResourceFiler, parser.getFacilityLocation(),parser.getBuildingLocation(),parser.getAmbulatoryLocation(),parser.getNurseUnitLocation(),parser.getRoomLocation(),parser.getBedLcoation()));

        fhirLocation.setManagingOrganization(ReferenceHelper.createReference(ResourceType.Organization, organisationResourceId.getResourceId().toString()));

        if (parentLocationResourceId != null) {
            fhirLocation.setPartOf(ReferenceHelper.createReference(ResourceType.Location, parentLocationResourceId));
        }

        LOG.debug("Save Location (LocationId=" + parser.getLocationId());
        saveAdminResource(fhirResourceFiler, parser.getCurrentState(), fhirLocation);

    }
    */


    /*
    *
     */
    private static String createSecondaryKey(String facilityCode, String buildingCode, String ambulatoryCode, String nurseUnitCode, String roomCode, String bedCode) {
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
    }

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
    private static String createDescription(FhirResourceFilerI fhirResourceFiler, CsvCell facilityCode, CsvCell buildingCode, CsvCell ambulatoryCode, CsvCell nurseUnitCode, CsvCell roomCode, CsvCell bedCode, ArrayList<CsvCell> dependencyList) throws Exception {
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
    }

    /*
     *
     */
    private static String getName(FhirResourceFilerI fhirResourceFiler, CsvCell facilityCode, CsvCell buildingCode, CsvCell ambulatoryCode, CsvCell nurseUnitCode, CsvCell roomCode, CsvCell bedCode, ArrayList<CsvCell> dependencyList) throws Exception {
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
    }

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
    private static String getCodeRefValue(FhirResourceFilerI fhirResourceFiler, String code) throws Exception {
        //LOG.debug("Looking for Cerner Code " + code + " in Code Set " + RdbmsCernerCodeValueRefDal.LOCATION_NAME + " for ServiceId " + fhirResourceFiler.getServiceId());
        CernerCodeValueRef cernerCodeDef = cernerCodeValueRefDAL.getCodeFromCodeSet(CernerCodeValueRef.LOCATION_NAME, Long.valueOf(code), fhirResourceFiler.getServiceId());
        if (cernerCodeDef != null) {
            return cernerCodeDef.getCodeDispTxt();
        } else {
            String ret = "??Unknown location " + code;
            // LOG.warn("Code not found in Code Value lookup:" + ret);
            return ret;
        }
    }
}
