package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.common.fhir.schema.ContactRelationship;
import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.common.fhir.schema.MaritalStatus;
import org.endeavourhealth.common.fhir.schema.NhsNumberVerificationStatus;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

import java.util.Date;

public class PatientBuilder extends ResourceBuilderBase
                            implements HasNameI,
                                       HasAddressI {

    private Patient patient = null;

    public PatientBuilder() {
        this(null);
    }

    public PatientBuilder(Patient patient) {
        this.patient = patient;
        if (this.patient == null) {
            this.patient = new Patient();
            this.patient.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_PATIENT));
        }
    }


    @Override
    public DomainResource getResource() {
        return patient;
    }


    public void addIdentifier(Identifier identifier, CsvCell... sourceCells) {
        this.patient.addIdentifier(identifier);

        int index = this.patient.getIdentifier().size()-1;
        auditValue("identifier[" + index + "].value", sourceCells);
    }

    public void setDateOfBirth(Date dob, CsvCell... sourceCells) {
        this.patient.setBirthDate(dob);

        auditValue("birthDate", sourceCells);
    }

    public void setDateOfDeath(Date dod, CsvCell... sourceCells) {
        this.patient.setDeceased(new DateTimeType(dod));

        auditValue("deceased.valueDateTime", sourceCells);
    }

    public void setGender(Enumerations.AdministrativeGender gender, CsvCell... sourceCells) {
        this.patient.setGender(gender);

        auditValue("gender", sourceCells);
    }

    public void setResidentialInstituteCode(String code, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateStringExtension(this.patient, FhirExtensionUri.PATIENT_RESIDENTIAL_INSTITUTE_CODE, code);

        auditStringExtension(extension, sourceCells);
    }

    public void addTelecom(ContactPoint contactPoint, CsvCell... sourceCells) {
        this.patient.addTelecom(contactPoint);

        int index = this.patient.getTelecom().size()-1;
        auditValue("telecom[" + index + "].value", sourceCells);
    }

    public void setManagingOrganisation(Reference organisationReference, CsvCell... sourceCells) {
        this.patient.setManagingOrganization(organisationReference);

        auditValue("managingOrganization.reference", sourceCells);
    }

    public void setSpineSensitive(boolean sensitive, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateBooleanExtension(this.patient, FhirExtensionUri.PATIENT_SPINE_SENSITIVE, sensitive);

        auditBooleanExtension(extension, sourceCells);
    }

    public void setActive(boolean active, CsvCell... sourceCells) {
        this.patient.setActive(active);

        auditValue("active", sourceCells);
    }

    public void setConfidential(boolean isConfidential, CsvCell... sourceCells) {
        createOrUpdateIsConfidentialExtension(isConfidential, sourceCells);
    }

    @Override
    public void addName(HumanName.NameUse use) {
        HumanName name = new HumanName();
        name.setUse(use);
        this.patient.addName(name);
    }

    @Override
    public HumanName getLastName() {
        return this.patient.getName().get(getLastNameIndex());
    }

    private int getLastNameIndex() {
        return this.patient.getName().size()-1;
    }

    @Override
    public void addNamePrefix(String prefix, CsvCell... sourceCells) {
        HumanName name = getLastName();
        name.addPrefix(prefix);

        int index = name.getPrefix().size()-1;
        auditValue("name[" + getLastNameIndex() + "].prefix[" + index + "]", sourceCells);

    }

    @Override
    public void addNameGiven(String given, CsvCell... sourceCells) {
        HumanName name = getLastName();
        name.addGiven(given);

        int index = name.getGiven().size()-1;
        auditValue("name[" + getLastNameIndex() + "].given[" + index + "]", sourceCells);
    }

    @Override
    public void addNameFamily(String family, CsvCell... sourceCells) {
        HumanName name = getLastName();
        name.addFamily(family);

        int index = name.getFamily().size()-1;
        auditValue("name[" + getLastNameIndex() + "].family[" + index + "]", sourceCells);
    }

    @Override
    public void addNameSuffix(String suffix, CsvCell... sourceCells) {
        HumanName name = getLastName();
        name.addSuffix(suffix);

        int index = name.getSuffix().size()-1;
        auditValue("name[" + getLastNameIndex() + "].suffix[" + index + "]", sourceCells);
    }

    @Override
    public void addNameDisplayName(String displayName, CsvCell... sourceCells) {
        HumanName name = getLastName();
        name.setText(displayName);

        auditValue("name[" + getLastNameIndex() + "].text", sourceCells);
    }

    @Override
    public void addAddress(Address.AddressUse use) {
        Address address = new Address();
        address.setUse(use);
        patient.getAddress().add(address);
    }

    @Override
    public Address getLastAddress() {
        return this.patient.getAddress().get(getLastAddressIndex());
    }

    private int getLastAddressIndex() {
        return this.patient.getAddress().size()-1;
    }

    @Override
    public void addAddressLine(String line, CsvCell... sourceCells) {
        Address address = getLastAddress();
        address.addLine(line);

        int index = address.getLine().size()-1;
        auditValue("address[" + getLastAddressIndex() + "].line[" + index + "]", sourceCells);
    }

    @Override
    public void addAddressTown(String town, CsvCell... sourceCells) {
        Address address = getLastAddress();
        address.setCity(town);

        auditValue("address[" + getLastAddressIndex() + "].city", sourceCells);
    }

    @Override
    public void addAddressDistrict(String district, CsvCell... sourceCells) {
        Address address = getLastAddress();
        address.setDistrict(district);

        auditValue("address[" + getLastAddressIndex() + "].district", sourceCells);
    }

    @Override
    public void addAddressPostcode(String postcode, CsvCell... sourceCells) {
        Address address = getLastAddress();
        address.setPostalCode(postcode);

        auditValue("address[" + getLastAddressIndex() + "].postalcode", sourceCells);
    }

    @Override
    public void addAddressDisplayText(String displayText, CsvCell... sourceCells) {
        Address address = getLastAddress();
        address.setText(displayText);

        auditValue("address[" + getLastAddressIndex() + "].text", sourceCells);
    }

    public void addCareProvider(Reference practitionerOrOrganizationReference, CsvCell... sourceCells) {
        this.patient.addCareProvider(practitionerOrOrganizationReference);

        int index = this.patient.getCareProvider().size()-1;
        auditValue("careProvider[" + index + "].reference", sourceCells);
    }

    public void setNhsNumberVerificationStatus(NhsNumberVerificationStatus verificationStatus, CsvCell... sourceCells) {
        CodeableConcept fhirCodeableConcept = CodeableConceptHelper.createCodeableConcept(verificationStatus);
        Extension extension = ExtensionConverter.createOrUpdateExtension(this.patient, FhirExtensionUri.PATIENT_NHS_NUMBER_VERIFICATION_STATUS, fhirCodeableConcept);

        auditCodeableConceptExtension(extension, sourceCells);
    }


    /**
     * called to add an empty contact (i.e. relationship with another person) which is then populated by the following fns
     */
    public void addContact() {
        this.patient.addContact();
    }

    private Patient.ContactComponent getLatestContact() {
        return this.patient.getContact().get(getLatestContactIndex());
    }

    private int getLatestContactIndex() {
        return this.patient.getContact().size()-1;
    }

    public void addContactName(HumanName humanName, CsvCell... sourceCells) {
        Patient.ContactComponent contact = getLatestContact();
        contact.setName(humanName);

        auditValue("contact[" + getLastAddressIndex() + "].name.text", sourceCells);
    }

    public void addContactRelationshipType(ContactRelationship fhirContactRelationship, CsvCell... sourceCells) {
        CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(fhirContactRelationship);
        Patient.ContactComponent contact = getLatestContact();
        contact.addRelationship(codeableConcept);

        auditValue("contact[" + getLastAddressIndex() + "].relationship.coding[0]", sourceCells);
    }

    public void addContactRelationshipTypeFreeText(String carerRelationshipFreeText, CsvCell... sourceCells) {
        CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(carerRelationshipFreeText);
        Patient.ContactComponent contact = getLatestContact();
        contact.addRelationship(codeableConcept);

        auditValue("contact[" + getLastAddressIndex() + "].relationship.text", sourceCells);
    }

    public void setMaritalStatus(MaritalStatus fhirMaritalStatus, CsvCell... sourceCells) {
        CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(fhirMaritalStatus);
        this.patient.setMaritalStatus(codeableConcept);

        auditValue("maritalStatus.coding[0]", sourceCells);
    }

    public void setEthnicity(EthnicCategory fhirEthnicity, CsvCell... sourceCells) {
        CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(fhirEthnicity);
        Extension extension = ExtensionConverter.createOrUpdateExtension(this.patient, FhirExtensionUri.PATIENT_ETHNICITY, codeableConcept);

        auditCodeableConceptExtension(extension, sourceCells);
    }

}
