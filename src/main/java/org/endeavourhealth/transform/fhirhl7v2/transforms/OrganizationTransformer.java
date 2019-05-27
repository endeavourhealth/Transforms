package org.endeavourhealth.transform.fhirhl7v2.transforms;

import org.endeavourhealth.common.fhir.AddressHelper;
import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.FhirValueSetUri;
import org.endeavourhealth.common.fhir.IdentifierHelper;
import org.endeavourhealth.common.fhir.schema.OrganisationType;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.AddressBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.OrganizationBuilder;
import org.hl7.fhir.instance.model.*;

import java.util.List;
import java.util.UUID;

public class OrganizationTransformer {

    private static final ResourceDalI resourceRepository = DalProvider.factoryResourceDal();
    
    public static Resource transform(Organization newOrg, FhirResourceFiler filer) throws Exception {
        UUID resourceId = UUID.fromString(newOrg.getId());
        ResourceWrapper wrapper = resourceRepository.getCurrentVersion(filer.getServiceId(), ResourceType.Patient.toString(), resourceId);

        OrganizationBuilder organizationBuilder = null;

        if (wrapper != null
                && wrapper.isDeleted()) {
            Organization existingPatient = (Organization) FhirSerializationHelper.deserializeResource(wrapper.getResourceData());
            organizationBuilder = new OrganizationBuilder(existingPatient);

        } else {
            organizationBuilder = new OrganizationBuilder();
        }

        //postcodes are sent through with spaces, which we remove everywhere else (if present), so do the same here
        tidyPostcodes(newOrg);

        updateIdentifiers(newOrg, organizationBuilder);
        updateType(newOrg, organizationBuilder);
        updateName(newOrg, organizationBuilder);
        updateAddress(newOrg, organizationBuilder);

        validateEmptyFields(newOrg);

        return organizationBuilder.getResource();
    }

    private static void validateEmptyFields(Organization newOrg) {
        if (newOrg.hasActiveElement()) {
            throw new RuntimeException("HL7 filer does not support updating Active element");
        }

        if (newOrg.hasTelecom()) {
            throw new RuntimeException("HL7 filer does not support updating Telecom element");
        }

        if (newOrg.hasPartOf()) {
            throw new RuntimeException("HL7 filer does not support updating PartOf element");
        }

        if (newOrg.hasContact()) {
            throw new RuntimeException("HL7 filer does not support updating Contact element");
        }

        if (newOrg.hasExtension()) {
            throw new RuntimeException("HL7 filer does not support updating Extensions");
        }
    }

    private static void updateAddress(Organization newOrg, OrganizationBuilder organizationBuilder) {

        //if the ADT patient doesn't have any Addresses, don't try to do anything to the existing Addresss.
        if (!newOrg.hasAddress()) {
            return;
        }

        Organization existingOrg = (Organization)organizationBuilder.getResource();

        //now add any Addresses from the new ADT patient if they don't already exist
        for (Address address: newOrg.getAddress()) {

            //if the Address already exists on the patient then we don't want to add it again
            List<Address> existingAddresss = AddressHelper.findMatches(address, existingOrg.getAddress());
            if (existingAddresss.isEmpty()) {

                //remove any existing addresses, since we only want one at any one time
                AddressBuilder.removeExistingAddresses(organizationBuilder);

                AddressBuilder addressBuilder = new AddressBuilder(organizationBuilder);
                addressBuilder.addAddressNoAudit(address);
            }
        }
    }

    private static void updateName(Organization newOrg, OrganizationBuilder organizationBuilder) {
        //if the ADT resource doesn't have a name, don't try to do anything to the existing identifiers.
        if (!newOrg.hasName()) {
            return;
        }

        String newName = newOrg.getName();

        Organization existingOrg = (Organization)organizationBuilder.getResource();
        String existingName = existingOrg.getName();

        if (existingName == null
                || !existingName.equalsIgnoreCase(newName)) {

            organizationBuilder.setName(newName);
        }
    }

    private static void updateType(Organization newOrg, OrganizationBuilder organizationBuilder) throws Exception {
        //if the ADT resource doesn't have a type, don't try to do anything to the existing identifiers.
        if (!newOrg.hasType()) {
            return;
        }

        OrganisationType newType = findOrgType(newOrg);

        Organization existingOrg = (Organization)organizationBuilder.getResource();
        OrganisationType existingType = findOrgType(existingOrg);
        
        if (existingType == null
                || existingType != newType) {
            
            organizationBuilder.setType(newType);
        }
    }

    private static OrganisationType findOrgType(Organization org) throws Exception {
        if (!org.hasType()) {
            return null;
        }

        CodeableConcept cc = org.getType();
        Coding coding = CodeableConceptHelper.findCoding(cc, FhirValueSetUri.VALUE_SET_ORGANISATION_TYPE);
        if (coding == null) {
            return null;
        }
        
        return OrganisationType.fromCode(coding.getCode());
    }

    private static void updateIdentifiers(Organization newOrg, OrganizationBuilder organizationBuilder) {

        //if the ADT resource doesn't have any identifiers, don't try to do anything to the existing identifiers.
        if (!newOrg.hasIdentifier()) {
            return;
        }

        Organization existingOrg = (Organization)organizationBuilder.getResource();

        //now add any identifiers from the new ADT patient if they don't already exist
        for (Identifier identifier: newOrg.getIdentifier()) {

            //if the identifier already exists on the patient then we don't want to add it again
            List<Identifier> existingIdentifiers = IdentifierHelper.findMatches(identifier, existingOrg.getIdentifier());
            if (existingIdentifiers.isEmpty()) {

                //make sure to remove any other identifiers with the same system before adding the new one
                IdentifierBuilder.removeExistingIdentifiersForSystem(organizationBuilder, identifier.getSystem());

                //add the new one
                IdentifierBuilder identifierBuilder = new IdentifierBuilder(organizationBuilder);
                identifierBuilder.addIdentifierNoAudit(identifier);
            }
        }

    }


    /**
     * removes spaces from address postcodes
     */
    private static void tidyPostcodes(Organization newOrg) {
        if (!newOrg.hasAddress()) {
            return;
        }

        for (Address address: newOrg.getAddress()) {
            if (address.hasPostalCode()) {
                String postcode = address.getPostalCode();
                postcode = postcode.replace(" ", "");
                address.setPostalCode(postcode);
            }
        }
    }
}
