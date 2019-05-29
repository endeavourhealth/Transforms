package org.endeavourhealth.transform.common.resourceBuilders;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class PatientContactBuilder implements HasNameI, HasAddressI, HasContactPointI, HasCodeableConceptI {
    private static final Logger LOG = LoggerFactory.getLogger(PatientContactBuilder.class);

    private PatientBuilder patientBuilder = null;
    private Patient.ContactComponent contact = null;

    public PatientContactBuilder(PatientBuilder patientBuilder) {
        this(patientBuilder, null);
    }

    public PatientContactBuilder(PatientBuilder patientBuilder, Patient.ContactComponent contact) {
        this.patientBuilder = patientBuilder;
        this.contact = contact;

        if (this.contact == null) {
            this.contact = patientBuilder.addContact();
        }
    }


    public static PatientContactBuilder findOrCreateForId(PatientBuilder parentBuilder, CsvCell idCell) {

        String idValue = idCell.getString();
        Patient.ContactComponent patientContact = findExistingContactPoint(parentBuilder, idValue);
        if (patientContact != null) {
            return new PatientContactBuilder(parentBuilder, patientContact);

        } else {
            PatientContactBuilder ret = new PatientContactBuilder(parentBuilder);
            ret.setId(idValue, idCell);
            return ret;
        }
    }

    private static Patient.ContactComponent findExistingContactPoint(PatientBuilder parentBuilder, String idValue) {
        if (Strings.isNullOrEmpty(idValue)) {
            throw new IllegalArgumentException("Can't look up patient contact without ID");
        }

        List<Patient.ContactComponent> matches = new ArrayList<>();

        List<Patient.ContactComponent> patientContacts = parentBuilder.getPatientContactComponents();
        //LOG.debug("Patient has " + patientContacts.size() + " relationships");
        for (Patient.ContactComponent patientContact: patientContacts) {
            //if we match on ID, then remove this patientContact from the parent object
            //LOG.debug("Relationship has ID " + patientContact.getId() + ", looking for " + idValue);
            if (patientContact.hasId()
                    && patientContact.getId().equals(idValue)) {

                matches.add(patientContact);
            }
        }

        if (matches.isEmpty()) {
            //LOG.debug("No matches found");
            return null;

        } else if (matches.size() > 1) {
            throw new IllegalArgumentException("Found " + matches.size() + " patientContacts for ID " + idValue);

        } else {
            return matches.get(0);
        }
    }



    public static boolean removeExistingContactPointById(PatientBuilder parentBuilder, String idValue) {

        Patient.ContactComponent patientContact = findExistingContactPoint(parentBuilder, idValue);
        if (patientContact != null) {

            //remove any audits we've created for the Name
            String identifierJsonPrefix = parentBuilder.getContactJsonPrefix(patientContact);
            parentBuilder.getAuditWrapper().removeAudit(identifierJsonPrefix);

            parentBuilder.removePatientContactComponent(patientContact);
            return true;

        } else {
            return false;
        }
    }



    public void setId(String idValue, CsvCell... sourceCells) {
        contact.setId(idValue);

        auditValue("id", sourceCells);
    }

    public void addContactName(HumanName humanName, CsvCell... sourceCells) {
        contact.setName(humanName);

        auditValue("name.text", sourceCells);
    }



    /*public void addContactRelationshipType(ContactRelationship fhirContactRelationship, CsvCell... sourceCells) {
        CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(fhirContactRelationship);
        contact.addRelationship(codeableConcept);

        int index = contact.getRelationship().indexOf(codeableConcept);
        auditValue("relationship[" + index + "].coding[0]", sourceCells);
    }

    public void addContactRelationshipTypeFreeText(String carerRelationshipFreeText, CsvCell... sourceCells) {
        CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(carerRelationshipFreeText);
        contact.addRelationship(codeableConcept);

        int index = contact.getRelationship().indexOf(codeableConcept);
        auditValue("relationship[" + index + "].text", sourceCells);
    }*/

    private void auditValue(String jsonSuffix, CsvCell... sourceCells) {

        String jsonField = patientBuilder.getContactJsonPrefix(contact) + "." + jsonSuffix;

        ResourceFieldMappingAudit audit = this.patientBuilder.getAuditWrapper();
        for (CsvCell csvCell: sourceCells) {
            if (csvCell != null) {
                if (csvCell.getOldStyleAuditId() != null) {
                    audit.auditValueOldStyle(csvCell.getOldStyleAuditId(), csvCell.getColIndex(), jsonField);
                } else {
                    audit.auditValue(csvCell.getPublishedFileId(), csvCell.getRecordNumber(), csvCell.getColIndex(), jsonField);
                }
            }
        }
    }


    @Override
    public Address addAddress() {
        if (contact.hasAddress()) {
            throw new IllegalArgumentException("Contact already has an address element");
        }

        Address address = new Address();
        contact.setAddress(address);
        return address;
    }

    @Override
    public String getAddressJsonPrefix(Address address) {
        return patientBuilder.getContactJsonPrefix(contact) + ".address";
    }

    @Override
    public ContactPoint addContactPoint() {
        return this.contact.addTelecom();
    }

    @Override
    public String getContactPointJsonPrefix(ContactPoint contactPoint) {
        int index = contact.getTelecom().indexOf(contactPoint);
        return patientBuilder.getContactJsonPrefix(contact) + ".telecom[" + index + "]";
    }

    @Override
    public HumanName addName() {
        if (contact.hasName()) {
            throw new IllegalArgumentException("Contact already has a name element");
        }

        HumanName name = new HumanName();
        contact.setName(name);
        return name;
    }

    @Override
    public String getNameJsonPrefix(HumanName name) {
        return patientBuilder.getContactJsonPrefix(contact) + ".name";
    }

    @Override
    public CodeableConcept createNewCodeableConcept(CodeableConceptBuilder.Tag tag, boolean useExisting) {
        if (tag == CodeableConceptBuilder.Tag.Patient_Contact_Relationship) {
            if (contact.hasRelationship()
                    && useExisting) {
                return contact.getRelationship().get(0);
            }

            CodeableConcept codeableConcept = contact.addRelationship();
            return codeableConcept;

        } else {
            throw new IllegalArgumentException("Unknown tag [" + tag + "]");
        }
    }

    @Override
    public String getCodeableConceptJsonPath(CodeableConceptBuilder.Tag tag, CodeableConcept codeableConcept) {
        if (tag == CodeableConceptBuilder.Tag.Patient_Contact_Relationship) {
            int index = contact.getRelationship().indexOf(codeableConcept);
            return patientBuilder.getContactJsonPrefix(contact) + ".relationship[" + index + "]";

        } else {
            throw new IllegalArgumentException("Unknown tag [" + tag + "]");
        }
    }


    @Override
    public ResourceFieldMappingAudit getAuditWrapper() {
        return patientBuilder.getAuditWrapper();
    }

    @Override
    public void removeCodeableConcept(CodeableConceptBuilder.Tag tag, CodeableConcept codeableConcept) {
        if (tag == CodeableConceptBuilder.Tag.Patient_Contact_Relationship) {
            this.contact.getRelationship().clear();

        } else {
            throw new IllegalArgumentException("Unknown tag [" + tag + "]");
        }
    }

    @Override
    public List<HumanName> getNames() {
        List<HumanName> ret = new ArrayList<>();
        if (this.contact.hasName()) {
            ret.add(this.contact.getName());
        }
        return ret;
    }

    @Override
    public void removeName(HumanName name) {
        this.contact.setName(null);
    }

    @Override
    public List<ContactPoint> getContactPoint() {
        return this.contact.getTelecom();
    }

    @Override
    public void removeContactPoint(ContactPoint contactPoint) {
        this.contact.getTelecom().remove(contactPoint);
    }

    @Override
    public List<Address> getAddresses() {
        List<Address> ret = new ArrayList<>();
        if (this.contact.hasAddress()) {
            ret.add(this.contact.getAddress());
        }
        return ret;
    }

    @Override
    public void removeAddress(Address address) {

        //TODO - audit removal

        this.contact.setAddress(null);
    }


    private Period getOrCreateNamePeriod() {
        Period period = null;
        if (contact.hasPeriod()) {
            period = contact.getPeriod();
        } else {
            period = new Period();
            contact.setPeriod(period);
        }
        return period;
    }

    public void setStartDate(Date date, CsvCell... sourceCells) {
        getOrCreateNamePeriod().setStart(date);

        auditValue("period.start", sourceCells);
    }

    public void setEndDate(Date date, CsvCell... sourceCells) {
        getOrCreateNamePeriod().setEnd(date);

        auditValue("period.end", sourceCells);
    }

    public void setLanguage(String language, CsvCell... sourceCells) {
        if (Strings.isNullOrEmpty(language)) {
            ExtensionConverter.removeExtension(this.contact, FhirExtensionUri.PATIENT_CONTACT_MAIN_LANGUAGE);

        } else {
            Extension extension = ExtensionConverter.createOrUpdateStringExtension(this.contact, FhirExtensionUri.PATIENT_CONTACT_MAIN_LANGUAGE, language);
            int index = this.contact.getExtension().indexOf(extension);
            auditValue("extension[" + index + "].valueString", sourceCells);
        }
    }

    public void setDateOfBirth(Date dob, CsvCell... sourceCells) {
        if (dob == null) {
            ExtensionConverter.removeExtension(this.contact, FhirExtensionUri.PATIENT_CONTACT_DOB);

        } else {
            Extension extension = ExtensionConverter.createOrUpdateExtension(this.contact, FhirExtensionUri.PATIENT_CONTACT_DOB, new DateType(dob));
            int index = this.contact.getExtension().indexOf(extension);
            auditValue("extension[" + index + "].valueDate", sourceCells);
        }
    }

    public void reset() {

        //this.contact.setId(null); //do not remove any ID as that's used to match names up
        this.contact.setAddress(null);
        this.contact.setName(null);
        this.contact.setPeriod(null);
        this.contact.setGender(null);
        this.contact.setOrganization(null);
        this.contact.getRelationship().clear();
        this.contact.getTelecom().clear();
    }

    public static void removeExistingContacts(PatientBuilder patientBuilder) {

        List<Patient.ContactComponent> contacts = new ArrayList<>(patientBuilder.getPatientContactComponents()); //need to copy the array so we can remove while iterating
        for (Patient.ContactComponent contact: contacts) {

            //remove any audits we've created for the Address
            String jsonPrefix = patientBuilder.getContactJsonPrefix(contact);
            patientBuilder.getAuditWrapper().removeAudit(jsonPrefix);

            patientBuilder.removePatientContactComponent(contact);
        }
    }

    public Patient.ContactComponent getContact() {
        return this.contact;
    }

    public void setGender(Enumerations.AdministrativeGender gender, CsvCell... sourceCells) {
        this.contact.setGender(gender);

        auditValue("gender", sourceCells);
    }
}
