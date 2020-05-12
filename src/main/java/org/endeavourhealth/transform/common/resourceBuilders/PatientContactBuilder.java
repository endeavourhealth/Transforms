package org.endeavourhealth.transform.common.resourceBuilders;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.PeriodHelper;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class PatientContactBuilder implements HasNameI, HasAddressI, HasContactPointI {
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
                audit.auditValue(csvCell.getPublishedFileId(), csvCell.getRecordNumber(), csvCell.getColIndex(), jsonField);
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

    /**
     * set the relationship between the individuals (e.g. parent)
     */
    public void setRelationship(String relationship, CsvCell... sourceCells) {
        if (Strings.isNullOrEmpty(relationship)) {
            contact.getRelationship().clear();
        } else {

            CodeableConcept codeableConcept = null;
            if (contact.hasRelationship()) {
                codeableConcept = contact.getRelationship().get(0);
            } else {
                codeableConcept = new CodeableConcept();
                contact.getRelationship().add(codeableConcept);
            }

            codeableConcept.setText(relationship);

            auditValue(patientBuilder.getContactJsonPrefix(contact) + ".relationship[0].text", sourceCells);
        }
    }

    @Override
    public ResourceFieldMappingAudit getAuditWrapper() {
        return patientBuilder.getAuditWrapper();
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
            Extension extension = ExtensionConverter.createOrUpdateExtension(this.contact, FhirExtensionUri.PATIENT_CONTACT_DOB, new DateTimeType(dob));
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


    /**
     * removes the last added contact point if it already exists in the resource, if not will end any
     * existing active ones. Anything added after the effective date will also be removed, to handle cases
     * where we're re-processing old data.
     */
    public static void deDuplicateLastPatientContact(PatientBuilder resource, Date effectiveDate) throws Exception {

        List<Patient.ContactComponent> contacts = resource.getPatientContactComponents();
        if (contacts.isEmpty()) {
            return;
        }

        Patient.ContactComponent lastPatientContactComponent = contacts.get(contacts.size()-1);

        //for feeds that have discrete records in the source data (e.g. TPP, Cerner) with their own unique IDs,
        //then this function isn't suitable as it's expected that individual records will be maintained using the ID.
        //Same goes for feeds that externally set dates on entries.
        if (lastPatientContactComponent.hasId()
                || lastPatientContactComponent.hasPeriod()) {
            throw new Exception("De-duplication function only expected to be used when no unique IDs or explicit dates available");
        }

        //make sure to roll back if we're re-processing old data
        rollBackToDate(resource, effectiveDate);

        List<Patient.ContactComponent> contactsToEnd = new ArrayList<>();
        boolean setStartDate = false;

        //note the start index is the one BEFORE the last one, above
        for (int i=contacts.size()-2; i>=0; i--) {
            Patient.ContactComponent contact = contacts.get(i);

            //if we've got previous history of entries in the same scope, then this is a delta and we can set the start date
            setStartDate = true;

            //ended ones shouldn't count towards the duplicate check
            if (!PeriodHelper.isActive(contact.getPeriod())) {
                continue;
            }

            //the shallow equals fn compares the value but not the period, which is what we want
            if (equalsIgnoringDate(contact, lastPatientContactComponent)) {
                //if the latest has same value as this existing active one, then it's a duplicate and should be removed
                contacts.remove(contacts.size() - 1);
                return;
            }

            //if we make it here, then this one should be ended
            contactsToEnd.add(contact);
        }

        if (setStartDate) {
            PatientContactBuilder builder = new PatientContactBuilder(resource, lastPatientContactComponent);
            builder.setStartDate(effectiveDate);
        }

        //end any active ones we've found
        if (!contactsToEnd.isEmpty()) {
            for (Patient.ContactComponent contactToEnd: contactsToEnd) {
                PatientContactBuilder builder = new PatientContactBuilder(resource, contactToEnd);
                builder.setEndDate(effectiveDate);
            }
        }
    }

    /**
     * tests if two contacts are the same, ignoring any Period elements
     */
    private static boolean equalsIgnoringDate(Patient.ContactComponent contact1, Patient.ContactComponent contact2) {

        String id1 = contact1.hasId() ? contact1.getId(): null;
        String id2 = contact2.hasId() ? contact2.getId(): null;
        if ((Strings.isNullOrEmpty(id1) && !Strings.isNullOrEmpty(id2))
            || (!Strings.isNullOrEmpty(id1) && Strings.isNullOrEmpty(id2))
            || (!Strings.isNullOrEmpty(id1) && !id1.equals(id2))) {
            return false;
        }

        HumanName name1 = contact1.hasName() ? contact1.getName(): null;
        HumanName name2 = contact2.hasName() ? contact2.getName(): null;
        if ((name1 == null && name2 != null)
                || (name1 != null && name2 == null)
                //use equalsShallow to compare the values but not the Period
                || (name1 != null && !name1.equalsShallow(name2))) {
            return false;
        }

        Address address1 = contact1.hasAddress() ? contact1.getAddress(): null;
        Address address2 = contact2.hasAddress() ? contact2.getAddress(): null;
        if ((address1 == null && address2 != null)
                || (address1 != null && address2 == null)
                //use equalsShallow to compare the values but not the Period
                || (address1 != null && !address1.equalsShallow(address2))) {
            return false;
        }

        Enumerations.AdministrativeGender gender1 = contact1.hasGender() ? contact1.getGender(): null;
        Enumerations.AdministrativeGender gender2 = contact2.hasGender() ? contact2.getGender(): null;
        if ((gender1 == null && gender2 != null)
                || (gender1 != null && gender2 == null)
                || (gender1 != gender2)) {
            return false;
        }

        Reference org1 = contact1.hasOrganization() ? contact1.getOrganization(): null;
        Reference org2 = contact2.hasOrganization() ? contact2.getOrganization(): null;
        if ((org1 == null && org2 != null)
                || (org1 != null && org2 == null)
                || (org1 != null && ReferenceHelper.equals(org1, org2))) {
            return false;
        }

        List<CodeableConcept> ccList1 = contact1.hasRelationship() ? contact1.getRelationship(): null;
        List<CodeableConcept> ccList2 = contact2.hasRelationship() ? contact2.getRelationship(): null;
        if ((ccList1 == null && ccList2 != null)
                || (ccList1 != null && ccList2 == null)
                || ccList1 != null && !Base.compareDeep(ccList1, ccList2, true)) {
            return false;
        }

        List<ContactPoint> telecomList1 = contact1.hasTelecom() ? contact1.getTelecom(): null;
        List<ContactPoint> telecomList2 = contact2.hasTelecom() ? contact2.getTelecom(): null;
        if ((telecomList1 == null && telecomList2 != null)
                || (telecomList1 != null && telecomList2 == null)
                || telecomList1 != null && !Base.compareDeep(telecomList1, telecomList2, true)) {
            return false;
        }

        //if we finally make it here, we're equivalent
        return true;
    }

    /**
     * if we know an contact is no longer active, this function will find any active contact (for the system and
     * use) and end it with the given date
     */
    public static void endPatientContacts(PatientBuilder resource, Date effectiveDate) throws Exception {
        List<Patient.ContactComponent> contacts = resource.getPatientContactComponents();
        if (contacts.isEmpty()) {
            return;
        }

        //make sure to roll back if we're re-processing old data
        rollBackToDate(resource, effectiveDate);

        for (int i=contacts.size()-1; i>=0; i--) {
            Patient.ContactComponent contact = contacts.get(i);
            if (PeriodHelper.isActive(contact.getPeriod())) {

                PatientContactBuilder builder = new PatientContactBuilder(resource, contact);
                builder.setEndDate(effectiveDate);
            }
        }
    }

    /**
     * because we sometimes need to re-process past data, we need this function to essentially roll back the
     * list to what it would have been on a given date. Removes anything known to have been added on or after
     * the effective date, and un-ends anything ended on or after that date.
     */
    private static void rollBackToDate(PatientBuilder resource, Date effectiveDate) throws Exception {

        List<Patient.ContactComponent> contacts = resource.getPatientContactComponents();
        for (int i=contacts.size()-1; i>=0; i--) {
            Patient.ContactComponent contact = contacts.get(i);
            if (contact.hasPeriod()) {
                Period p = contact.getPeriod();

                //if it was added on or after the effective date, remove it
                if (p.hasStart()
                        && !p.getStart().before(effectiveDate)) {
                    contacts.remove(i);
                    continue;
                }

                //if it was ended on or after the effective date, un-end it
                if (p.hasEnd()
                        && !p.getEnd().before(effectiveDate)) {
                    PatientContactBuilder builder = new PatientContactBuilder(resource, contact);
                    builder.setEndDate(null);
                }
            }
        }
    }    
}
