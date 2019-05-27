package org.endeavourhealth.transform.fhirhl7v2.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.common.fhir.schema.MaritalStatus;
import org.endeavourhealth.common.fhir.schema.Religion;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.ehr.ResourceDalI;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.*;
import org.hl7.fhir.instance.model.*;

import java.util.*;

public class PatientTransformer {

    private static final ResourceDalI resourceRepository = DalProvider.factoryResourceDal();

    public static Patient updatePatient(Patient newPatient, FhirResourceFiler filer, Date adtDate, Bundle bundle) throws Exception {

        UUID resourceId = UUID.fromString(newPatient.getId());
        ResourceWrapper wrapper = resourceRepository.getCurrentVersion(filer.getServiceId(), ResourceType.Patient.toString(), resourceId);

        PatientBuilder patientBuilder = null;

        if (wrapper != null
                && wrapper.isDeleted()) {
            Patient existingPatient = (Patient) FhirSerializationHelper.deserializeResource(wrapper.getResourceData());
            patientBuilder = new PatientBuilder(existingPatient);

        } else {
            patientBuilder = new PatientBuilder();
        }

        //the HL7 Receiver brings through email addresses as Addresses, rather than as ContactPoint elements
        //so before doing any processing, fix that
        moveEmailAddress(newPatient);

        //the ADT feed sends NHS numbers with spaces in them (or at least used to), so ensure they're all formatted without them
        tidyNhsNumbers(newPatient);

        //postcodes are sent through with spaces, which we remove everywhere else (if present), so do the same here
        tidyPostcodes(newPatient);

        updateExtensions(newPatient, patientBuilder, adtDate);
        updateGender(newPatient, patientBuilder);
        updateBirthDate(newPatient, patientBuilder);
        updateMaritalStatus(newPatient, patientBuilder);
        updatePatientNames(newPatient, patientBuilder, adtDate);
        updatePatientAddresses(newPatient, patientBuilder, adtDate);
        updatePatientTelecoms(newPatient, patientBuilder, adtDate);
        updateDeathDate(newPatient, patientBuilder);
        updateIdentifiers(newPatient, patientBuilder, adtDate);
        updateManagingOrganisation(newPatient, patientBuilder);
        updateCommunication(newPatient, patientBuilder);
        updateCareProviders(newPatient, patientBuilder, bundle, filer.getServiceId());

        validateEmptyPatientFields(newPatient);

        return (Patient)patientBuilder.getResource();
    }

    /**
     * removes spaces from address postcodes
     */
    private static void tidyPostcodes(Patient newPatient) {
        if (!newPatient.hasAddress()) {
            return;
        }

        for (Address address: newPatient.getAddress()) {
            if (address.hasPostalCode()) {
                String postcode = address.getPostalCode();
                postcode = postcode.replace(" ", "");
                address.setPostalCode(postcode);
            }
        }
    }

    /**
     * detects an email address in the Address list and moves to be a ContactPoint in the telecom list
     */
    private static void moveEmailAddress(Patient newPatient) {
        if (!newPatient.hasAddress()) {
            return;
        }

        String emailToAdd = null;

        //see if we have an address that looks like an email
        for (int i=newPatient.getAddress().size()-1; i>=0; i--) {
            Address address = newPatient.getAddress().get(i);

            if (!address.hasPostalCode()
                    && !address.hasCity()
                    && !address.hasUse()) {

                for (StringType st: address.getLine()) {
                    String s = st.toString();
                    if (s.contains("@")) {
                        emailToAdd = s;
                        break;
                    }
                }
            }

            if (emailToAdd != null) {
                newPatient.getAddress().remove(i);
                break;
            }
        }

        if (emailToAdd == null) {
            return;
        }

        PatientBuilder patientBuilder = new PatientBuilder(newPatient);
        ContactPointBuilder contactPointBuilder = new ContactPointBuilder(patientBuilder);

        contactPointBuilder.setSystem(ContactPoint.ContactPointSystem.EMAIL);
        contactPointBuilder.setUse(ContactPoint.ContactPointUse.HOME);
        contactPointBuilder.setValue(emailToAdd);
    }

    private static void updateCommunication(Patient newPatient, PatientBuilder patientBuilder) {

        CodeableConcept newLanguage = findLanguageCodeableConcept(newPatient);
        if (newLanguage == null) {
            return;
        }
        String newLanguageText = newLanguage.getText();
        if (Strings.isNullOrEmpty(newLanguageText)) {
            return;
        }

        //the Data Warehouse and ADT feeds using different coding systems for patient language, but they
        //have the same textual descriptions. So compare the codeable concept text before deciding whether to apply
        //the new one or not
        Patient existingPatient = (Patient)patientBuilder.getResource();
        CodeableConcept existingLanguage = findLanguageCodeableConcept(existingPatient);
        if (existingLanguage != null) {
            String existingLanguageText = existingLanguage.getText();
            if (!Strings.isNullOrEmpty(existingLanguageText)
                    && existingLanguageText.equalsIgnoreCase(newLanguageText)) {
                return;
            }
        }

        //remove existing one
        CodeableConceptBuilder.removeExistingCodeableConcept(patientBuilder, CodeableConceptBuilder.Tag.Patient_Language, existingLanguage);

        //add new one
        CodeableConceptBuilder cc = new CodeableConceptBuilder(patientBuilder, CodeableConceptBuilder.Tag.Patient_Language);

        //the HL7 codeable concept has multiple codings, only one of which is useful, so only carry over the one with the system
        for (Coding newCoding: newLanguage.getCoding()) {

            //skip ones without systems
            if (newCoding.hasSystem()) {
                cc.addCoding(newCoding.getSystem());

                if (newCoding.hasCode()) {
                    cc.setCodingCode(newCoding.getCode());
                }
                if (newCoding.hasDisplay()) {
                    cc.setCodingDisplay(newCoding.getDisplay());
                }
            }
        }

        cc.setText(newLanguageText);
    }

    private static CodeableConcept findLanguageCodeableConcept(Patient patient) {
        if (!patient.hasCommunication()) {
            return null;
        }
        List<Patient.PatientCommunicationComponent> newComms = patient.getCommunication();
        Patient.PatientCommunicationComponent newComm = newComms.get(0);
        if (!newComm.hasLanguage()) {
            return null;
        }
        return newComm.getLanguage();
    }

    private static void updateManagingOrganisation(Patient newPatient, PatientBuilder patientBuilder) {

        if (!newPatient.hasManagingOrganization()) {
            return;
        }

        //the managing organisation only needs to be set once, so don't change it. This also gets around
        //the problem of the HL7 Receiver using a different national ID to the data warehouse data (R1H vs RNJ)
        Patient existingPatient = (Patient)patientBuilder.getResource();
        if (existingPatient.hasManagingOrganization()) {
            return;
        }

        Reference ref = newPatient.getManagingOrganization();
        patientBuilder.setManagingOrganisation(ref);
    }

    private static void updateIdentifiers(Patient newPatient, PatientBuilder patientBuilder, Date adtDate) {

        //if the ADT patient doesn't have any identifiers, don't try to do anything to the existing identifiers.
        if (!newPatient.hasIdentifier()) {
            return;
        }

        Patient existingPatient = (Patient)patientBuilder.getResource();

        //the ADT patient only contains "current" identifiers, so remove any existing identifiers from a previous ADT message (i.e. w/o an ID)
        //that aren't in the new patient. Iterate backwards, since we're removing.
        for (int i=existingPatient.getIdentifier().size()-1; i>=0; i--) {
            Identifier identifier = existingPatient.getIdentifier().get(i);

            //if this identifier has an ID, then it's from the DW feed, so don't do anything with it
            if (identifier.hasId()) {
                continue;
            }

            //see if this identifier exists in the new ADT patient
            List<Identifier> duplicatesInNew = IdentifierHelper.findMatches(identifier, newPatient.getIdentifier());
            if (duplicatesInNew.isEmpty()) {
                //if it's not in the ADT patient, remove
                existingPatient.getIdentifier().remove(i);
            }
        }

        //now add any identifiers from the new ADT patient if they don't already exist
        for (Identifier identifier: newPatient.getIdentifier()) {

            boolean addIdentifier = true;

            //if the identifier already exists on the patient then we don't want to add it again
            List<Identifier> existingIdentifiers = IdentifierHelper.findMatches(identifier, existingPatient.getIdentifier());
            if (!existingIdentifiers.isEmpty()) {

                for (Identifier existingIdentifier: existingIdentifiers) {

                    Period existingIdentifierPeriod = existingIdentifier.getPeriod();
                    boolean isActive = PeriodHelper.isActive(existingIdentifierPeriod, adtDate); //use the ADT date in case we're processing an old message
                    if (isActive) {
                        addIdentifier = false;
                        break;
                    }
                }
            }

            if (addIdentifier) {
                IdentifierBuilder identifierBuilder = new IdentifierBuilder(patientBuilder);
                identifierBuilder.addIdentifierNoAudit(identifier);
            }
        }
    }

    private static void updateCareProviders(Patient newPatient, PatientBuilder patientBuilder, Bundle bundle, UUID serviceId) throws Exception {

        //if the updated patient doesn't have any care providers (reg GP or practice), just ignore it
        if (!newPatient.hasCareProvider()) {
            return;
        }

        ReferenceComponents newOrgRef = findCareProviderReference(newPatient, ResourceType.Organization);
        ReferenceComponents newPractitionerRef = findCareProviderReference(newPatient, ResourceType.Practitioner);

        Patient existingPatient = (Patient)patientBuilder.getResource();
        ReferenceComponents existingOrgRef = findCareProviderReference(existingPatient, ResourceType.Organization);
        ReferenceComponents existingPractitionerRef = findCareProviderReference(existingPatient, ResourceType.Practitioner);

        if (newOrgRef != null) {
            boolean add = true;

            //we need to compare ODS codes because the two feeds have their own instances of organisational objects
            //otherwise we just end up constantly swapping the care provider references between ADT and DW spaces
            if (existingOrgRef != null) {

                Organization newOrg = (Organization)findBundleResource(serviceId, bundle, newOrgRef);
                if (newOrg == null) {
                    throw new RuntimeException("Failed to find " + newOrgRef);
                }
                String newOds = IdentifierHelper.findOdsCode(newOrg);

                Organization existingOrg = (Organization)findExistingResource(serviceId, existingOrgRef);
                String existingOds = null;
                if (existingOrg != null) { //if we've got bad data, deal with it as the new data will fix it
                    existingOds = IdentifierHelper.findOdsCode(existingOrg);
                }

                if (existingOds != null
                        && existingOds.equalsIgnoreCase(newOds)) {
                    add = false;
                }
            }

            if (add) {
                patientBuilder.removeCareProvider(ReferenceHelper.createReference(existingOrgRef));
                patientBuilder.addCareProvider(ReferenceHelper.createReference(newOrgRef));

                //if the org has changed, then carry over the new practitioner reference. This does mean that if
                //the practitioner reference changes but NOT the ods one, then this update will be ignored. Howerver,
                //this will be corrected when the PPAGP file is next processed. The ADT and DW feeds use different
                //ways of populating the practitioner resources, so it's not simple to compare them to see if they're the same person
                patientBuilder.removeCareProvider(ReferenceHelper.createReference(existingPractitionerRef));
                patientBuilder.addCareProvider(ReferenceHelper.createReference(newPractitionerRef));
            }
        }

    }


    private static Resource findExistingResource(UUID serviceId, ReferenceComponents referenceComponents) throws Exception {
        return resourceRepository.getCurrentVersionAsResource(serviceId, referenceComponents.getResourceType(), referenceComponents.getId());
    }

    private static Resource findBundleResource(UUID serviceId, Bundle bundle, ReferenceComponents referenceComponents) throws Exception {
        for (Bundle.BundleEntryComponent entry: bundle.getEntry()) {
            Resource r = entry.getResource();
            if (r.getResourceType() == referenceComponents.getResourceType()
                    && r.getId().equals(referenceComponents.getId())) {
                return r;
            }
        }

        //not sure if the bundle will ever NOT contain a referenced resource, but if it does, hit the DB
        return findExistingResource(serviceId, referenceComponents);
    }

    private static ReferenceComponents findCareProviderReference(Patient patient, ResourceType resourceType) {

        if (!patient.hasCareProvider()) {
            return null;
        }

        for (Reference reference: patient.getCareProvider()) {
            ReferenceComponents comps = ReferenceHelper.getReferenceComponents(reference);
            if (comps.getResourceType() == resourceType) {
                return comps;
            }
        }

        return null;
    }

    private static void updateDeathDate(Patient newPatient, PatientBuilder patientBuilder) {

        //the deceased element can be a date or a boolean, and both may be set in the Data Warehouse transform (but not at the same time)
        Type deceased = newPatient.getDeceased();
        if (deceased == null) {
            patientBuilder.clearDateOfDeath();

        } else if (deceased instanceof DateTimeType) {
            DateTimeType dt = (DateTimeType)deceased;
            patientBuilder.setDateOfDeath(dt.getValue());

        } else if (deceased instanceof BooleanType) {
            BooleanType bt = (BooleanType)deceased;
            if (bt.getValue()) {
                patientBuilder.setDateOfDeathBoolean(true);
            } else {
                patientBuilder.clearDateOfDeath();
            }

        } else {
            throw new RuntimeException("Unexpected type in deceased field " + deceased.getClass().getName());
        }
    }

    private static void updateMaritalStatus(Patient newPatient, PatientBuilder patientBuilder) {

        MaritalStatus newMaritalStatus = new PatientBuilder(newPatient).getMaritalStatus();

        //the value set URL was wrong in the HL7 receiver until May 2019, so if we reprocess any old
        //HL7 messages, we need this code to find the old ones
        if (newMaritalStatus == null) {
            CodeableConcept cc = newPatient.getMaritalStatus();
            if (cc != null) {
                Coding coding = CodeableConceptHelper.findCoding(cc, "http://hl7.org/fhir/marital-status");
                if (coding != null) {
                    newMaritalStatus = MaritalStatus.fromCode(coding.getCode());
                }
            }
        }

        MaritalStatus currentMaritalStatus = patientBuilder.getMaritalStatus();

        if (newMaritalStatus != currentMaritalStatus) {
            patientBuilder.setMaritalStatus(newMaritalStatus);
        }
    }

    private static void updateBirthDate(Patient newPatient, PatientBuilder patientBuilder) {

        //if the new patient has not DoB, then don't clear anything
        /*if (newPatient.hasBirthDate()) {
            return;
        }*/

        Date newDob = newPatient.getBirthDate();
        Date currentDob = patientBuilder.getDateOfBirth();

        if (currentDob == null
                || !newDob.equals(currentDob)) {

            patientBuilder.setDateOfBirth(newDob);
        }
    }

    private static void updateGender(Patient newPatient, PatientBuilder patientBuilder) {

        //don't update anything if we've not got a new gender
        /*if (!newPatient.hasGender()) {
            return;
        }*/

        Enumerations.AdministrativeGender newGender = newPatient.getGender();
        Enumerations.AdministrativeGender currentGender = patientBuilder.getGender();

        if (newGender != currentGender) {
            patientBuilder.setGender(newGender);
        }
    }

    private static void updateExtensions(Patient newPatient, PatientBuilder patientBuilder, Date adtDate) {

        //we have a number of known patient extensions but do not expect the ADT patients to have them
        assertNoExtension(newPatient, FhirExtensionUri.PATIENT_RESIDENTIAL_INSTITUTE_CODE);
        assertNoExtension(newPatient, FhirExtensionUri.PATIENT_SPINE_SENSITIVE);
        assertNoExtension(newPatient, FhirExtensionUri.PATIENT_IS_TEST_PATIENT);
        assertNoExtension(newPatient, FhirExtensionUri.PATIENT_SPEAKS_ENGLISH);
        assertNoExtension(newPatient, FhirExtensionUri.PATIENT_NHS_NUMBER_VERIFICATION_STATUS);

        Set<String> knownExtensions = new HashSet<>();
        knownExtensions.add(FhirExtensionUri.PATIENT_ETHNICITY);
        knownExtensions.add(FhirExtensionUri.PATIENT_RELIGION);
        assertNoUnexpectedExtension(newPatient, knownExtensions);

        updateEthnicity(newPatient, patientBuilder, adtDate);
        updateReligion(newPatient, patientBuilder, adtDate);
    }

    private static void updateReligion(Patient newPatient, PatientBuilder patientBuilder, Date adtDate) {

        //quick and easy way to get the ethnicity is to simply wrap the new patient in a builder
        Religion newReligion = new PatientBuilder(newPatient).getReligion();
        String newFreeTextReligion = new PatientBuilder(newPatient).getReligionFreeText();

        Religion currentReligion = patientBuilder.getReligion();
        String currentFreeTextReligion = patientBuilder.getReligionFreeText();

        if (newReligion != null) {
            //only change if different
            if (newReligion != currentReligion) {
                patientBuilder.setReligion(newReligion);
            }

        } else if (!Strings.isNullOrEmpty(newFreeTextReligion)) {

            //only change if different
            if (currentFreeTextReligion == null
                    || !newFreeTextReligion.equalsIgnoreCase(currentFreeTextReligion)) {
                patientBuilder.setReligionFreeText(newFreeTextReligion);
            }

        } else {
            //clear the religion
            patientBuilder.setReligion(null);
        }

    }

    private static void updateEthnicity(Patient newPatient, PatientBuilder patientBuilder, Date adtDate) {

        //quick and easy way to get the ethnicity is to simply wrap the new patient in a builder
        EthnicCategory newEthnicity = new PatientBuilder(newPatient).getEthnicity();

        EthnicCategory currentEthnicity = patientBuilder.getEthnicity();

        //only change if different (which may be setting it to null)
        if (newEthnicity != currentEthnicity) {
            patientBuilder.setEthnicity(newEthnicity);
        }
    }

    private static void assertNoUnexpectedExtension(Patient newPatient, Set<String> knownExtensions) {
        for (Extension extension: newPatient.getExtension()) {
            String extensionUrl = extension.getUrl();
            if (!knownExtensions.contains(extensionUrl)) {
                throw new RuntimeException("Unexpected extension in ADT patient " + extensionUrl);
            }
        }
    }

    private static void assertNoExtension(Patient newPatient, String extensionUrl) {

        Extension extension = ExtensionConverter.findExtension(newPatient, extensionUrl);
        if (extension != null) {
            throw new RuntimeException("HL7 filer doesn't handle patients with extension " + extensionUrl);
        }
    }

    /**
     * ensures that fields we expect to be empty actually are
     */
    private static void validateEmptyPatientFields(Patient newPatient) {

        if (newPatient.hasContact()) {
            throw new RuntimeException("HL7 filer does not support updating patientContacts");
        }

        if (newPatient.hasActiveElement()) {
            throw new RuntimeException("HL7 filer does not support updating active status");
        }

        if (newPatient.hasMultipleBirth()) {
            throw new RuntimeException("HL7 filer does not support updating multiple birth data");
        }

        if (newPatient.hasPhoto()) {
            throw new RuntimeException("HL7 filer does not support updating photo data");
        }

        if (newPatient.hasAnimal()) {
            throw new RuntimeException("HL7 filer does not support updating animal data");
        }

        if (newPatient.hasLink()) {
            throw new RuntimeException("HL7 filer does not support updating linked patient data");
        }
    }

    private static void updatePatientTelecoms(Patient newPatient, PatientBuilder patientBuilder, Date adtDate) {

        //if the ADT patient doesn't have any ContactPoints, don't try to do anything to the existing ContactPoints.
        if (!newPatient.hasTelecom()) {
            return;
        }

        Patient existingPatient = (Patient)patientBuilder.getResource();

        //the ADT patient only contains "current" ContactPoints, so remove any existing ContactPoints from a previous ADT message (i.e. w/o an ID)
        //that aren't in the new patient. Iterate backwards, since we're removing.
        for (int i=existingPatient.getTelecom().size()-1; i>=0; i--) {
            ContactPoint telecom = existingPatient.getTelecom().get(i);

            //if this ContactPoint has an ID, then it's from the DW feed, so don't do anything with it
            if (telecom.hasId()) {
                continue;
            }

            //see if this ContactPoint exists in the new ADT patient
            List<ContactPoint> duplicatesInNew = ContactPointHelper.findMatches(telecom, newPatient.getTelecom());
            if (duplicatesInNew.isEmpty()) {
                //if it's not in the ADT patient, remove
                existingPatient.getTelecom().remove(i);
            }
        }

        //now add any ContactPointes from the new ADT patient if they don't already exist
        for (ContactPoint telecom: newPatient.getTelecom()) {

            boolean addContactPoint = true;

            //if the ContactPoint already exists on the patient then we don't want to add it again
            List<ContactPoint> existingContactPoints = ContactPointHelper.findMatches(telecom, existingPatient.getTelecom());
            if (!existingContactPoints.isEmpty()) {

                for (ContactPoint existingContactPoint: existingContactPoints) {

                    Period existingContactPointPeriod = existingContactPoint.getPeriod();
                    boolean isActive = PeriodHelper.isActive(existingContactPointPeriod, adtDate); //use the ADT date in case we're processing an old message
                    if (isActive) {
                        addContactPoint = false;
                        break;
                    }
                }
            }

            if (addContactPoint) {
                ContactPointBuilder telecomBuilder = new ContactPointBuilder(patientBuilder);
                telecomBuilder.addContactPointNoAudit(telecom);
            }
        }
    }

    private static void updatePatientAddresses(Patient newPatient, PatientBuilder patientBuilder, Date adtDate) {

        //if the ADT patient doesn't have any Addresses, don't try to do anything to the existing Addresss.
        if (!newPatient.hasAddress()) {
            return;
        }

        Patient existingPatient = (Patient)patientBuilder.getResource();

        //the ADT patient only contains "current" Addresses, so remove any existing Addresses from a previous ADT message (i.e. w/o an ID)
        //that aren't in the new patient. Iterate backwards, since we're removing.
        for (int i=existingPatient.getAddress().size()-1; i>=0; i--) {
            Address address = existingPatient.getAddress().get(i);

            //if this Address has an ID, then it's from the DW feed, so don't do anything with it
            if (address.hasId()) {
                continue;
            }

            //see if this Address exists in the new ADT patient
            List<Address> duplicatesInNew = AddressHelper.findMatches(address, newPatient.getAddress());
            if (duplicatesInNew.isEmpty()) {
                //if it's not in the ADT patient, remove
                existingPatient.getAddress().remove(i);
            }
        }

        //now add any Addresses from the new ADT patient if they don't already exist
        for (Address address: newPatient.getAddress()) {

            boolean addAddress = true;

            //if the Address already exists on the patient then we don't want to add it again
            List<Address> existingAddresss = AddressHelper.findMatches(address, existingPatient.getAddress());
            if (!existingAddresss.isEmpty()) {

                for (Address existingAddress: existingAddresss) {

                    Period existingAddressPeriod = existingAddress.getPeriod();
                    boolean isActive = PeriodHelper.isActive(existingAddressPeriod, adtDate); //use the ADT date in case we're processing an old message
                    if (isActive) {
                        addAddress = false;
                        break;
                    }
                }
            }

            if (addAddress) {
                AddressBuilder addressBuilder = new AddressBuilder(patientBuilder);
                addressBuilder.addAddressNoAudit(address);
            }
        }

    }

    private static void updatePatientNames(Patient newPatient, PatientBuilder patientBuilder, Date adtDate) {

        //if the ADT patient doesn't have any names, don't try to do anything to the existing names.
        if (!newPatient.hasName()) {
            return;
        }

        Patient existingPatient = (Patient)patientBuilder.getResource();

        //the ADT patient only contains "current" names, so remove any existing names from a previous ADT message (i.e. w/o an ID)
        //that aren't in the new patient. Iterate backwards, since we're removing.
        for (int i=existingPatient.getName().size()-1; i>=0; i--) {
            HumanName name = existingPatient.getName().get(i);

            //if this name has an ID, then it's from the DW feed, so don't do anything with it
            if (name.hasId()) {
                continue;
            }

            //see if this name exists in the new ADT patient
            List<HumanName> duplicatesInNew = NameHelper.findMatches(name, newPatient.getName());
            if (duplicatesInNew.isEmpty()) {
                //if it's not in the ADT patient, remove
                existingPatient.getName().remove(i);
            }
        }

        //now add any names from the new ADT patient if they don't already exist
        for (HumanName name: newPatient.getName()) {

            boolean addName = true;

            //if the name already exists on the patient then we don't want to add it again
            List<HumanName> existingNames = NameHelper.findMatches(name, existingPatient.getName());
            if (!existingNames.isEmpty()) {

                for (HumanName existingName: existingNames) {

                    Period existingNamePeriod = existingName.getPeriod();
                    boolean isActive = PeriodHelper.isActive(existingNamePeriod, adtDate); //use the ADT date in case we're processing an old message
                    if (isActive) {
                        addName = false;
                        break;
                    }
                }
            }

            if (addName) {
                NameBuilder nameBuilder = new NameBuilder(patientBuilder);
                nameBuilder.addNameNoAudit(name);
            }
        }

    }

    /**
     * the HL7 Receiver leaves spaces in NHS numbers, which nothing else does, so remove them before saving
     */
    private static void tidyNhsNumbers(Patient patient) {
        if (!patient.hasIdentifier()) {
            return;
        }

        for (Identifier identifier: patient.getIdentifier()) {
            if (identifier.getSystem().equals(FhirIdentifierUri.IDENTIFIER_SYSTEM_NHSNUMBER)) {
                String value = identifier.getValue();
                value = value.replace(" ", "");
                identifier.setValue(value);
            }
        }
    }

}
