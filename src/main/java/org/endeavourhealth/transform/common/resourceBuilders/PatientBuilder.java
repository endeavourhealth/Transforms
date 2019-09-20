package org.endeavourhealth.transform.common.resourceBuilders;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.EthnicCategory;
import org.endeavourhealth.common.fhir.schema.MaritalStatus;
import org.endeavourhealth.common.fhir.schema.NhsNumberVerificationStatus;
import org.endeavourhealth.common.fhir.schema.Religion;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class PatientBuilder extends ResourceBuilderBase
                            implements HasNameI,
                                       HasAddressI,
                                       HasIdentifierI,
                                       HasContactPointI,
                                       HasCodeableConceptI {



    private Patient patient = null;

    public PatientBuilder() {
        this(null);
    }

    public PatientBuilder(Patient patient) {
        this(patient, null);
    }

    public PatientBuilder(Patient patient, ResourceFieldMappingAudit audit) {
        super(audit);

        this.patient = patient;
        if (this.patient == null) {
            this.patient = new Patient();
            this.patient.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_PATIENT));
        }
    }

    @Override
    public DomainResource getResource() {
        return patient;
    }



    public void setDateOfBirth(Date dob, CsvCell... sourceCells) {
        if (dob == null) {
            this.patient.setBirthDate(null);

            //there's also an extension used to store a date and TIME of birth, so remove that too
            ExtensionConverter.removeExtension(this.patient, FhirExtensionUri.PATIENT_BIRTH_DATE_TIME);

        } else {
            this.patient.setBirthDate(dob);

            auditValue("birthDate", sourceCells);

            //if the dob has a time component, then set the Date in the date and TIME extension too
            Calendar cal = Calendar.getInstance();
            cal.setTime(dob);
            if (cal.get(Calendar.HOUR_OF_DAY) > 0
                    || cal.get(Calendar.MINUTE) > 0) {
                ExtensionConverter.createOrUpdateDateTimeExtension(this.patient, FhirExtensionUri.PATIENT_BIRTH_DATE_TIME, dob);

            } else {
                ExtensionConverter.removeExtension(this.patient, FhirExtensionUri.PATIENT_BIRTH_DATE_TIME);
            }
        }
    }

    public Date getDateOfBirth() {
        return this.patient.getBirthDate();
    }

    public void clearDateOfDeath() {

        //if we've already set any death details, we'll have audited it. So remove them.
        getAuditWrapper().removeAudit("deceasedBoolean");
        getAuditWrapper().removeAudit("deceasedDateTime");

        this.patient.setDeceased(null);
    }

    public void setDateOfDeath(Date dod, CsvCell... sourceCells) {
        if (dod == null) {
            throw new IllegalArgumentException("Use clearDateOfDeath to remove date of death");
        }
        this.patient.setDeceased(new DateTimeType(dod));

        auditValue("deceasedDateTime", sourceCells);
    }

    public void setDateOfDeathBoolean(boolean deceased, CsvCell... sourceCells) {
        this.patient.setDeceased(new BooleanType(deceased));

        auditValue("deceasedBoolean", sourceCells);
    }


    public Enumerations.AdministrativeGender getGender() {
        return this.patient.getGender();
    }

    public void setGender(Enumerations.AdministrativeGender gender, CsvCell... sourceCells) {
        if (gender == null) {
            this.patient.setGender(null);

        } else {
            this.patient.setGender(gender);

            auditValue("gender", sourceCells);
        }
    }

    public void setResidentialInstituteCode(String code, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateStringExtension(this.patient, FhirExtensionUri.PATIENT_RESIDENTIAL_INSTITUTE_CODE, code);

        auditStringExtension(extension, sourceCells);
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

    public void clearCareProvider() {
        if (this.patient.hasCareProvider()) {
            this.patient.getCareProvider().clear();
        }
    }

    public void addCareProvider(Reference practitionerOrOrganizationReference, CsvCell... sourceCells) {
        //only add to the patient if not already present
        for (Reference reference: this.patient.getCareProvider()) {
            if (ReferenceHelper.equals(reference, practitionerOrOrganizationReference)) {
                return;
            }
        }

        this.patient.addCareProvider(practitionerOrOrganizationReference);

        int index = this.patient.getCareProvider().size()-1;
        auditValue("careProvider[" + index + "].reference", sourceCells);
    }

    public void removeCareProvider(Reference practitionerOrOrganizationReference) {

        if (practitionerOrOrganizationReference == null) {
            return;
        }

        //if we've already set the care provider and then want to clear it, we need to remove any audits
        getAuditWrapper().removeAudit("careProvider");

        //can't just remove from the list because the Reference class doesn't implement equals(..) properly
        for (int i=this.patient.getCareProvider().size()-1; i>=0; i--) {
            Reference reference = patient.getCareProvider().get(i);
            if (ReferenceHelper.equals(reference, practitionerOrOrganizationReference)) {
                this.patient.getCareProvider().remove(i);
            }
        }

        //we can't audit anything against this change because we can't audit against something that isn't there
    }

    public void removeAllCareProviders() {
        //if we've already set the care provider and then want to clear it, we need to remove any audits
        getAuditWrapper().removeAudit("careProvider");

        this.patient.getCareProvider().clear();
    }


    public void setNhsNumberVerificationStatus(NhsNumberVerificationStatus verificationStatus, CsvCell... sourceCells) {
        //we may be updating a resource, so if null is passed in, we want to REMOVE it
        if (verificationStatus == null) {
            ExtensionConverter.removeExtension(this.patient, FhirExtensionUri.PATIENT_NHS_NUMBER_VERIFICATION_STATUS);

        } else {
            CodeableConcept fhirCodeableConcept = CodeableConceptHelper.createCodeableConcept(verificationStatus);
            Extension extension = ExtensionConverter.createOrUpdateExtension(this.patient, FhirExtensionUri.PATIENT_NHS_NUMBER_VERIFICATION_STATUS, fhirCodeableConcept);

            auditCodeableConceptExtension(extension, sourceCells);
        }
    }


    public Patient.ContactComponent addContact() {
        return this.patient.addContact();
    }

    public String getContactJsonPrefix(Patient.ContactComponent contactComponent) {
        int index = this.patient.getContact().indexOf(contactComponent);
        return "contact[" + index + "]";
    }

    /**
     * called to add an empty contact (i.e. relationship with another person) which is then populated by the following fns
     */
    /*public void addContact() {
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

        int index = contact.getRelationship().indexOf(codeableConcept);
        auditValue("contact[" + getLastAddressIndex() + "].relationship[" + index + "].coding[0]", sourceCells);
    }

    public void addContactRelationshipTypeFreeText(String carerRelationshipFreeText, CsvCell... sourceCells) {
        CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(carerRelationshipFreeText);
        Patient.ContactComponent contact = getLatestContact();
        contact.addRelationship(codeableConcept);

        int index = contact.getRelationship().indexOf(codeableConcept);
        auditValue("contact[" + getLastAddressIndex() + "].relationship[" + index + "].text", sourceCells);
    }*/

    public MaritalStatus getMaritalStatus() {
        CodeableConcept codeableConcept = this.patient.getMaritalStatus();
        if (codeableConcept == null) {
            return null;
        }

        Coding coding = CodeableConceptHelper.findCoding(codeableConcept, MaritalStatus.ANNULLED.getSystem()); //can use the system from any of the enum values
        if (coding == null) {
            return null;
        }

        return MaritalStatus.fromCode(coding.getCode());
    }

    public void setMaritalStatus(MaritalStatus fhirMaritalStatus, CsvCell... sourceCells) {
        if (fhirMaritalStatus == null) {
            this.patient.setMaritalStatus(null);

        } else {
            CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(fhirMaritalStatus);
            this.patient.setMaritalStatus(codeableConcept);

            auditValue("maritalStatus.coding[0]", sourceCells);
        }
    }

    public EthnicCategory getEthnicity() {
        CodeableConcept codeableConcept = (CodeableConcept)ExtensionConverter.findExtensionValue(this.patient, FhirExtensionUri.PATIENT_ETHNICITY);
        if (codeableConcept == null) {
            return null;
        }

        Coding coding = CodeableConceptHelper.findCoding(codeableConcept, EthnicCategory.WHITE_BRITISH.getSystem()); //can get the system URL from any of the enum
        if (coding == null) {
            return null;
        }

        return EthnicCategory.fromCode(coding.getCode());
    }

    public void setEthnicity(EthnicCategory fhirEthnicity, CsvCell... sourceCells) {
        if (fhirEthnicity == null) {
            ExtensionConverter.removeExtension(this.patient, FhirExtensionUri.PATIENT_ETHNICITY);

        } else {
            CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(fhirEthnicity);
            Extension extension = ExtensionConverter.createOrUpdateExtension(this.patient, FhirExtensionUri.PATIENT_ETHNICITY, codeableConcept);
            auditCodeableConceptExtension(extension, sourceCells);
        }
    }


    public Religion getReligion() {
        CodeableConcept codeableConcept = (CodeableConcept)ExtensionConverter.findExtensionValue(this.patient, FhirExtensionUri.PATIENT_RELIGION);
        if (codeableConcept == null) {
            return null;
        }

        Coding coding = CodeableConceptHelper.findCoding(codeableConcept, Religion.RASTAFARI.getSystem()); //can get the system URL from any of the enum
        if (coding == null
                || !coding.hasCode()) {
            return null;
        }

        return Religion.fromCode(coding.getCode());
    }

    public void setReligion(Religion fhirReligion, CsvCell... sourceCells) {
        if (fhirReligion == null) {
            ExtensionConverter.removeExtension(this.patient, FhirExtensionUri.PATIENT_RELIGION);

        } else {
            CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(fhirReligion);
            Extension extension = ExtensionConverter.createOrUpdateExtension(this.patient, FhirExtensionUri.PATIENT_RELIGION, codeableConcept);
            auditCodeableConceptExtension(extension, sourceCells);
        }
    }

    /**
     * if possible, set the religion using one of the Religion enum values. Only if not possible to map, use this free-text version
     */
    public void setReligionFreeText(String freeTextReligion, CsvCell... sourceCells) {
        if (Strings.isNullOrEmpty((freeTextReligion))) {
            ExtensionConverter.removeExtension(this.patient, FhirExtensionUri.PATIENT_RELIGION);

        } else {
            CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(freeTextReligion);
            Extension extension = ExtensionConverter.createOrUpdateExtension(this.patient, FhirExtensionUri.PATIENT_RELIGION, codeableConcept);
            auditCodeableConceptExtension(extension, sourceCells);
        }
    }

    public String getReligionFreeText() {
        CodeableConcept codeableConcept = (CodeableConcept)ExtensionConverter.findExtensionValue(this.patient, FhirExtensionUri.PATIENT_RELIGION);
        if (codeableConcept == null) {
            return null;
        }

        //check for a coding that uses our value set. If present, then we don't have a free-text religion and are using the value set
        Coding coding = CodeableConceptHelper.findCoding(codeableConcept, Religion.RASTAFARI.getSystem()); //can get the system URL from any of the enum
        if (coding != null) {
            return null;
        }

        return codeableConcept.getText();
    }

    @Override
    public Identifier addIdentifier() {
        return this.patient.addIdentifier();
    }

    @Override
    public String getIdentifierJsonPrefix(Identifier identifier) {
        int index = this.patient.getIdentifier().indexOf(identifier);
        return "identifier[" + index + "]";
    }

    @Override
    public ContactPoint addContactPoint() {
        return this.patient.addTelecom();
    }

    @Override
    public String getContactPointJsonPrefix(ContactPoint contactPoint) {
        int index = this.patient.getTelecom().indexOf(contactPoint);
        return "telecom[" + index + "]";
    }

    @Override
    public List<ContactPoint> getContactPoint() {
        return this.patient.getTelecom();
    }

    @Override
    public void removeContactPoint(ContactPoint contactPoint) {
        this.patient.getTelecom().remove(contactPoint);
    }

    /**
     * although FHIR supports multiple communication components, this builder doesn't. If needed, then change.
     */
    private Patient.PatientCommunicationComponent getOrCreateCommunicationComponent(boolean createIfMissing) {
        Patient.PatientCommunicationComponent communicationComponent = null;
        if (this.patient.getCommunication().isEmpty()) {
            if (createIfMissing) {
                communicationComponent = this.patient.addCommunication();
            }
        } else {
            communicationComponent = this.patient.getCommunication().get(0);
        }
        return communicationComponent;
    }

    @Override
    public CodeableConcept createNewCodeableConcept(CodeableConceptBuilder.Tag tag, boolean useExisting) {
        if (tag == CodeableConceptBuilder.Tag.Patient_Language) {
            Patient.PatientCommunicationComponent communicationComponent = getOrCreateCommunicationComponent(true);
            if (communicationComponent.hasLanguage()) {
                if (useExisting) {
                    return communicationComponent.getLanguage();
                }
            }
            communicationComponent.setLanguage(new CodeableConcept());

            //NOTE this is an assumption that when we record a patient's language, it's the preferred one
            //If we need more control over this, the creation of the Patient Communication should
            //be refactored out into a PatientCommunicationBuilder class to expose this for setting differently
            communicationComponent.setPreferred(true);

            return communicationComponent.getLanguage();

        /*} else if (tag == CodeableConceptBuilder.Tag.Patient_Religion) {
            Extension extension = ExtensionConverter.findOrCreateExtension(this.patient, FhirExtensionUri.PATIENT_RELIGION);
            if (extension.hasValue()) {
                if (useExisting) {
                    return (CodeableConcept)extension.getValue();
                } else {
                    ExtensionConverter.removeExtension(this.patient, FhirExtensionUri.PATIENT_RELIGION);
                }
            }
            CodeableConcept ret = new CodeableConcept();
            extension.setValue(ret);
            return ret;*/

        } else {
            throw new IllegalArgumentException("Unknown tag [" + tag + "]");
        }
    }

    @Override
    public String getCodeableConceptJsonPath(CodeableConceptBuilder.Tag tag, CodeableConcept codeableConcept) {
        if (tag == CodeableConceptBuilder.Tag.Patient_Language) {
            return "communication[0].language";

        /*} else if (tag == CodeableConceptBuilder.Tag.Patient_Religion) {
            Extension extension = ExtensionConverter.findOrCreateExtension(this.patient, FhirExtensionUri.PATIENT_RELIGION);
            int index = this.patient.getExtension().indexOf(extension);
            return "extension[" + index + "].valueCodeableConcept";*/

        } else {
            throw new IllegalArgumentException("Unknown tag [" + tag + "]");
        }
    }

    @Override
    public void removeCodeableConcept(CodeableConceptBuilder.Tag tag, CodeableConcept codeableConcept) {

        if (tag == CodeableConceptBuilder.Tag.Patient_Language) {
            Patient.PatientCommunicationComponent communicationComponent = getOrCreateCommunicationComponent(false);
            if (communicationComponent != null) {
                communicationComponent.setLanguage(null);
                communicationComponent.setPreferredElement(null); //to properly null this, we need to access the element

                //and remove from the patient if it's now empty
                if (communicationComponent.isEmpty()) {
                    this.patient.getCommunication().remove(communicationComponent);
                }
            }

        /*} else if (tag == CodeableConceptBuilder.Tag.Patient_Religion) {
            ExtensionConverter.removeExtension(this.patient, FhirExtensionUri.PATIENT_RELIGION);*/

        } else {
            throw new IllegalArgumentException("Unknown tag [" + tag + "]");
        }
    }

    @Override
    public Address addAddress() {
        Address address = new Address();
        this.patient.addAddress(address);
        return address;
    }

    @Override
    public String getAddressJsonPrefix(Address address) {
        int index = this.patient.getAddress().indexOf(address);
        return "address[" + index + "]";
    }

    @Override
    public List<Address> getAddresses() {
        return this.patient.getAddress();
    }

    @Override
    public void removeAddress(Address address) {
        this.patient.getAddress().remove(address);
    }

    @Override
    public HumanName addName() {
        return this.patient.addName();
    }

    @Override
    public String getNameJsonPrefix(HumanName name) {
        int index = this.patient.getName().indexOf(name);
        return "name[" + index + "]";
    }

    @Override
    public List<HumanName> getNames() {
        return this.patient.getName();
    }

    @Override
    public void removeName(HumanName name) {
        this.patient.getName().remove(name);
    }

    @Override
    public List<Identifier> getIdentifiers() {
        return this.patient.getIdentifier();
    }

    @Override
    public void removeIdentifier(Identifier identifier) {
        this.patient.getIdentifier().remove(identifier);
    }

    public List<Patient.ContactComponent> getPatientContactComponents() {
        return this.patient.getContact();
    }

    public void removePatientContactComponent(Patient.ContactComponent patientContact) {
        this.patient.getContact().remove(patientContact);
    }

    public void setSpeaksEnglish(Boolean speaksEnglish, CsvCell... sourceCells) {

        if (speaksEnglish != null) {
            Extension extension = ExtensionConverter.createOrUpdateBooleanExtension(this.patient, FhirExtensionUri.PATIENT_SPEAKS_ENGLISH, speaksEnglish.booleanValue());

            auditBooleanExtension(extension, sourceCells);

        } else {

            ExtensionConverter.removeExtension(this.patient, FhirExtensionUri.PATIENT_SPEAKS_ENGLISH);
        }
    }

    public void setTestPatient(boolean testPatient, CsvCell... sourceCells) {

        //only add the extension if the patient IS a test patient
        if (testPatient) {
            Extension extension = ExtensionConverter.createOrUpdateBooleanExtension(this.patient, FhirExtensionUri.PATIENT_IS_TEST_PATIENT, true);
            auditBooleanExtension(extension, sourceCells);

        } else {
            ExtensionConverter.removeExtension(this.patient, FhirExtensionUri.PATIENT_IS_TEST_PATIENT);
        }
    }

    /**
     * use speaksEnglish
     */
    /*public void setInterpreterRequired(Boolean interpreterRequired, CsvCell... sourceCells) {

        if (interpreterRequired != null) {
            Extension extension = ExtensionConverter.createOrUpdateBooleanExtension(this.patient, FhirExtensionUri.PATIENT_INTERPRETER_REQUIRED, interpreterRequired.booleanValue());

            auditBooleanExtension(extension, sourceCells);

        } else {

            ExtensionConverter.removeExtension(this.patient, FhirExtensionUri.PATIENT_INTERPRETER_REQUIRED);
        }
    }*/

    public boolean hasManagingOrganisation() {
        return this.patient.hasManagingOrganization();
    }
}
