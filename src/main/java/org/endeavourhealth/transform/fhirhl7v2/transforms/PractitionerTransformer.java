package org.endeavourhealth.transform.fhirhl7v2.transforms;

import org.endeavourhealth.common.fhir.FhirIdentifierUri;
import org.endeavourhealth.common.fhir.IdentifierHelper;
import org.endeavourhealth.common.fhir.NameHelper;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.IdentifierBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.NameBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.PractitionerBuilder;
import org.hl7.fhir.instance.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class PractitionerTransformer {

    private static final ResourceDalI resourceRepository = DalProvider.factoryResourceDal();

    public static Resource transform(Practitioner newPractitioner, FhirResourceFiler filer) throws Exception {
        UUID resourceId = UUID.fromString(newPractitioner.getId());
        ResourceWrapper wrapper = resourceRepository.getCurrentVersion(filer.getServiceId(), ResourceType.Patient.toString(), resourceId);

        PractitionerBuilder practitionerBuilder = null;

        if (wrapper != null
                && wrapper.isDeleted()) {
            Practitioner existingPatient = (Practitioner) FhirSerializationHelper.deserializeResource(wrapper.getResourceData());
            practitionerBuilder = new PractitionerBuilder(existingPatient);

        } else {
            practitionerBuilder = new PractitionerBuilder();
        }

        //postcodes are sent through with spaces, which we remove everywhere else (if present), so do the same here
        tidyIdentifiers(newPractitioner);

        updateIdentifiers(newPractitioner, practitionerBuilder);
        updateName(newPractitioner, practitionerBuilder);
        updateRole(newPractitioner, practitionerBuilder);

        validateEmptyFields(newPractitioner);


        return practitionerBuilder.getResource();
    }

    private static void validateEmptyFields(Practitioner newPractitioner) {
        if (newPractitioner.hasActiveElement()) {
            throw new RuntimeException("HL7 filer does not support updating Active element");
        }

        if (newPractitioner.hasTelecom()) {
            throw new RuntimeException("HL7 filer does not support updating Telecom elements");
        }

        if (newPractitioner.hasAddress()) {
            throw new RuntimeException("HL7 filer does not support updating Address elements");
        }

        if (newPractitioner.hasGender()) {
            throw new RuntimeException("HL7 filer does not support updating Gender elements");
        }

        if (newPractitioner.hasBirthDate()) {
            throw new RuntimeException("HL7 filer does not support updating BirthDate elements");
        }

        if (newPractitioner.hasPhoto()) {
            throw new RuntimeException("HL7 filer does not support updating Photo elements");
        }

        if (newPractitioner.hasQualification()) {
            throw new RuntimeException("HL7 filer does not support updating Qualification elements");
        }

        if (newPractitioner.hasCommunication()) {
            throw new RuntimeException("HL7 filer does not support updating Communication elements");
        }

        if (newPractitioner.hasExtension()) {
            throw new RuntimeException("HL7 filer does not support updating Extensions");
        }
    }

    private static void updateRole(Practitioner newPractitioner, PractitionerBuilder practitionerBuilder) {
        //if the ADT patient doesn't have any roles, don't try to do anything to the existing names.
        if (!newPractitioner.hasPractitionerRole()) {
            return;
        }

        //we only ever expect a single role from ADT. If this changes, the below code will need to get smarter.
        if (newPractitioner.getPractitionerRole().size() > 1) {
            throw new RuntimeException("Multiple roles found on practitioner");
        }
        Practitioner.PractitionerPractitionerRoleComponent newRole = newPractitioner.getPractitionerRole().get(0);

        //the below is a shortcut because we know that the Data Warehouse feed doesn't populate roles. If it ever does,
        //then the below code will need to be changed to check for differences properly, like the other
        //fields where both feeds overlap
        Practitioner existingPractitioner = (Practitioner)practitionerBuilder.getResource();
        existingPractitioner.getPractitionerRole().clear();
        existingPractitioner.getPractitionerRole().add(newRole);
    }

    private static void updateName(Practitioner newPractitioner, PractitionerBuilder practitionerBuilder) {
        //if the ADT patient doesn't have any names, don't try to do anything to the existing names.
        if (!newPractitioner.hasName()) {
            return;
        }

        Practitioner existingPractitioner = (Practitioner)practitionerBuilder.getResource();
        List<HumanName> l = new ArrayList<>();
        if (existingPractitioner.hasName()) {
            l.add(existingPractitioner.getName());
        }

        HumanName name = newPractitioner.getName();

        //if the name already exists on the patient then we don't want to add it again
        List<HumanName> existingNames = NameHelper.findMatches(name, l);
        if (existingNames.isEmpty()) {

            //remove any existing names first, as we only want to keep ont
            NameBuilder.removeExistingNames(practitionerBuilder);

            NameBuilder nameBuilder = new NameBuilder(practitionerBuilder);
            nameBuilder.addNameNoAudit(name);
        }
    }

    private static void updateIdentifiers(Practitioner newPractitioner, PractitionerBuilder practitionerBuilder) {

        //if the ADT resource doesn't have any identifiers, don't try to do anything to the existing identifiers.
        if (!newPractitioner.hasIdentifier()) {
            return;
        }

        Organization existingOrg = (Organization)practitionerBuilder.getResource();

        //now add any identifiers from the new ADT patient if they don't already exist
        for (Identifier identifier: newPractitioner.getIdentifier()) {

            //if the identifier already exists on the patient then we don't want to add it again
            List<Identifier> existingIdentifiers = IdentifierHelper.findMatches(identifier, existingOrg.getIdentifier());
            if (existingIdentifiers.isEmpty()) {

                //make sure to remove any other identifiers with the same system before adding the new one
                IdentifierBuilder.removeExistingIdentifiersForSystem(practitionerBuilder, identifier.getSystem());

                //add the new one
                IdentifierBuilder identifierBuilder = new IdentifierBuilder(practitionerBuilder);
                identifierBuilder.addIdentifierNoAudit(identifier);
            }
        }
    }

    private static void tidyIdentifiers(Practitioner newPractitioner) {
        if (!newPractitioner.hasIdentifier()) {
            return;
        }

        //the HL7 Receiver wrongly transforms GMP numbers as GMC numbers (they're related, as the GMP
        //number is the GMC number with a G at the start). So fix any we receive.
        for (Identifier identifier: newPractitioner.getIdentifier()) {
            String system = identifier.getSystem();
            if (system.equals(FhirIdentifierUri.IDENTIFIER_SYSTEM_GMC_NUMBER)) {
                String value = identifier.getValue();
                if (value.startsWith("G")) {
                    identifier.setSystem(FhirIdentifierUri.IDENTIFIER_SYSTEM_GMP_PPD_CODE);
                }
            }
        }
    }
}
