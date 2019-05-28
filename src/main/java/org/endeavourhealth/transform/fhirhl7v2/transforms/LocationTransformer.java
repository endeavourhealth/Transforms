package org.endeavourhealth.transform.fhirhl7v2.transforms;

import org.endeavourhealth.common.fhir.AddressHelper;
import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.IdentifierHelper;
import org.endeavourhealth.common.fhir.schema.LocationPhysicalType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.AddressBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.LocationBuilder;
import org.hl7.fhir.instance.model.*;
import org.hl7.fhir.instance.model.valuesets.V3RoleCode;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class LocationTransformer {

    private static final ResourceDalI resourceRepository = DalProvider.factoryResourceDal();

    public static Resource transform(Location newLocation, FhirResourceFiler filer) throws Exception {
        UUID resourceId = UUID.fromString(newLocation.getId());
        ResourceWrapper wrapper = resourceRepository.getCurrentVersion(filer.getServiceId(), newLocation.getResourceType().toString(), resourceId);

        LocationBuilder locationBuilder = null;

        if (wrapper != null
                && !wrapper.isDeleted()) {
            Location existingPatient = (Location) FhirSerializationHelper.deserializeResource(wrapper.getResourceData());
            locationBuilder = new LocationBuilder(existingPatient);

        } else {
            locationBuilder = new LocationBuilder();
            locationBuilder.setId(resourceId.toString());
        }

        //postcodes are sent through with spaces, which we remove everywhere else (if present), so do the same here
        tidyPostcodes(newLocation);

        updateIdentifiers(newLocation, locationBuilder);
        updateStatus(newLocation, locationBuilder);
        updateName(newLocation, locationBuilder);
        updateMode(newLocation, locationBuilder);
        updateType(newLocation, locationBuilder);
        updateAddress(newLocation, locationBuilder);
        updatePhysicalType(newLocation, locationBuilder);
        updateManagingOrganisation(newLocation, locationBuilder);

        validateEmptyFields(newLocation);

        return locationBuilder.getResource();
    }

    private static void updateManagingOrganisation(Location newLocation, LocationBuilder locationBuilder) {
        if (!newLocation.hasManagingOrganization()) {
            return;
        }

        Reference newRef = newLocation.getManagingOrganization();
        Reference existingRef = locationBuilder.getManagingOrganisation();

        if (existingRef == null
                || !newRef.getReference().equals(existingRef.getReference())) { //note, need to compare inner Strings, as equals(..) isn't implemented
            locationBuilder.setManagingOrganisation(newRef);
        }

    }

    private static void updatePhysicalType(Location newLocation, LocationBuilder locationBuilder) {
        //if the ADT resource doesn't have a type, don't try to do anything to the existing identifiers.
        if (!newLocation.hasPhysicalType()) {
            return;
        }

        LocationPhysicalType newPhysicalType = new LocationBuilder(newLocation).getPhysicalType();
        LocationPhysicalType existingPhysicalType = locationBuilder.getPhysicalType();

        if (existingPhysicalType == null
                || existingPhysicalType != newPhysicalType) {

            locationBuilder.setPhysicalType(newPhysicalType);
        }
    }

    private static void updateMode(Location newLocation, LocationBuilder locationBuilder) {
        if (!newLocation.hasMode()) {
            return;
        }

        Location.LocationMode newMode = newLocation.getMode();
        Location.LocationMode existingMode = locationBuilder.getMode();

        if (existingMode == null
                || newMode != existingMode) {
            locationBuilder.setMode(newMode);
        }
    }

    private static void updateStatus(Location newLocation, LocationBuilder locationBuilder) {
        if (!newLocation.hasStatus()) {
            return;
        }

        Location.LocationStatus newStatus = newLocation.getStatus();
        Location.LocationStatus existingStatus = locationBuilder.getStatus();

        if (existingStatus == null
                || newStatus != existingStatus) {
            locationBuilder.setStatus(newStatus);
        }
    }

    private static void validateEmptyFields(Location newLocation) {
        if (newLocation.hasDescription()) {
            throw new RuntimeException("HL7 filer does not support updating Description element");
        }

        if (newLocation.hasTelecom()) {
            throw new RuntimeException("HL7 filer does not support updating Telecom element");
        }

        if (newLocation.hasPosition()) {
            throw new RuntimeException("HL7 filer does not support updating Position element");
        }

        if (newLocation.hasPartOf()) {
            throw new RuntimeException("HL7 filer does not support updating PartOf element");
        }

        if (newLocation.hasExtension()) {
            throw new RuntimeException("HL7 filer does not support updating Extensions");
        }
    }

    private static void updateAddress(Location newLocation, LocationBuilder locationBuilder) {

        //if the ADT patient doesn't have any Addresses, don't try to do anything to the existing Addresss.
        if (!newLocation.hasAddress()) {
            return;
        }

        Address address = newLocation.getAddress();

        //if the Address already exists on the patient then we don't want to add it again
        Location existingOrg = (Location)locationBuilder.getResource();
        List<Address> l = new ArrayList<>();
        if (existingOrg.hasAddress()) {
            l.add(existingOrg.getAddress());
        }
        List<Address> existingAddresss = AddressHelper.findMatches(address, l);
        if (existingAddresss.isEmpty()) {

            //remove any existing addresses, since we only want one at any one time
            AddressBuilder.removeExistingAddresses(locationBuilder);

            AddressBuilder addressBuilder = new AddressBuilder(locationBuilder);
            addressBuilder.addAddressNoAudit(address);
        }
    }

    private static void updateName(Location newLocation, LocationBuilder locationBuilder) {
        //if the ADT resource doesn't have a name, don't try to do anything to the existing identifiers.
        if (!newLocation.hasName()) {
            return;
        }

        String newName = newLocation.getName();

        Location existingOrg = (Location)locationBuilder.getResource();
        String existingName = existingOrg.getName();

        if (existingName == null
                || !existingName.equalsIgnoreCase(newName)) {

            locationBuilder.setName(newName);
        }
    }

    private static void updateType(Location newLocation, LocationBuilder locationBuilder) throws Exception {
        //if the ADT resource doesn't have a type, don't try to do anything to the existing identifiers.
        V3RoleCode newType = findLocationType(newLocation);
        if (newType == null) {
            return;
        }

        Location existingLocation = (Location)locationBuilder.getResource();
        V3RoleCode existingType = findLocationType(existingLocation);

        if (existingType == null
                || existingType != newType) {

            CodeableConceptBuilder cc = new CodeableConceptBuilder(locationBuilder, CodeableConceptBuilder.Tag.Location_Type, true);

            //only add a coding if not already present
            if (!cc.hasCoding()) {
                cc.addCoding(newType.getSystem());
            }
            cc.setCodingCode(newType.toCode());
            cc.setCodingDisplay(newType.getDisplay());
            cc.setText(newType.getDisplay());
        }
    }

    private static V3RoleCode findLocationType(Location location) throws Exception {
        if (!location.hasType()) {
            return null;
        }

        CodeableConcept cc = location.getType();
        Coding coding = CodeableConceptHelper.findCoding(cc, V3RoleCode.HOSP.getSystem()); //can use any of the enums to get the system
        if (coding == null) {
            return null;
        }

        return V3RoleCode.fromCode(coding.getCode());
    }

    private static void updateIdentifiers(Location newLocation, LocationBuilder locationBuilder) {

        //if the ADT resource doesn't have any identifiers, don't try to do anything to the existing identifiers.
        if (!newLocation.hasIdentifier()) {
            return;
        }

        Location existingOrg = (Location)locationBuilder.getResource();

        //now add any identifiers from the new ADT patient if they don't already exist
        for (Identifier identifier: newLocation.getIdentifier()) {

            //if the identifier already exists on the patient then we don't want to add it again
            List<Identifier> existingIdentifiers = IdentifierHelper.findMatches(identifier, existingOrg.getIdentifier());
            if (existingIdentifiers.isEmpty()) {

                //make sure to remove any other identifiers with the same system before adding the new one
                IdentifierBuilder.removeExistingIdentifiersForSystem(locationBuilder, identifier.getSystem());

                //add the new one
                IdentifierBuilder identifierBuilder = new IdentifierBuilder(locationBuilder);
                identifierBuilder.addIdentifierNoAudit(identifier);
            }
        }

    }


    /**
     * removes spaces from address postcodes
     */
    private static void tidyPostcodes(Location newLocation) {
        if (!newLocation.hasAddress()) {
            return;
        }

        Address address = newLocation.getAddress();
        if (address.hasPostalCode()) {
            String postcode = address.getPostalCode();
            postcode = postcode.replace(" ", "");
            address.setPostalCode(postcode);
        }
    }
}
