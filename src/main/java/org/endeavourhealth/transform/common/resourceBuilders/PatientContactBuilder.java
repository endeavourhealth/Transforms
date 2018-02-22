package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

import java.util.Date;

public class PatientContactBuilder implements HasNameI, HasAddressI, HasContactPointI, HasCodeableConceptI {

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
            audit.auditValue(csvCell.getRowAuditId(), csvCell.getColIndex(), jsonField);
        }
    }

    @Override
    public void addAddress(Address.AddressUse use) {
        if (contact.hasAddress()) {
            throw new IllegalArgumentException("Contact already has an address element");
        }

        Address address = new Address();
        address.setUse(use);
        contact.setAddress(address);
    }

    @Override
    public Address getLastAddress() {
        return contact.getAddress();
    }

    @Override
    public String getLastAddressJsonPrefix() {
        return patientBuilder.getContactJsonPrefix(contact) + ".address";
    }

    @Override
    public void addContactPoint() {
        this.contact.addTelecom();
    }

    private int getLastContactPointIndex() {
        return contact.getTelecom().size()-1;
    }

    @Override
    public ContactPoint getLastContactPoint() {
        int index = getLastContactPointIndex();
        return contact.getTelecom().get(index);
    }

    @Override
    public String getLastContactPointJsonPrefix() {
        int index = getLastContactPointIndex();
        return patientBuilder.getContactJsonPrefix(contact) + ".telecom[" + index + "]";
    }

    @Override
    public void addName(HumanName.NameUse use) {
        if (contact.hasName()) {
            throw new IllegalArgumentException("Contact already has a name element");
        }

        HumanName name = new HumanName();
        name.setUse(use);
        contact.setName(name);
    }

    @Override
    public HumanName getLastName() {
        return contact.getName();
    }

    @Override
    public String getLastNameJsonPrefix() {
        return patientBuilder.getContactJsonPrefix(contact) + ".name";
    }


    @Override
    public CodeableConcept createNewCodeableConcept(String tag) {
        CodeableConcept codeableConcept = contact.addRelationship();
        return codeableConcept;
    }

    @Override
    public String getCodeableConceptJsonPath(String tag, CodeableConcept codeableConcept) {
        int index = contact.getRelationship().indexOf(codeableConcept);
        return "relationship[" + index + "]";
    }

    @Override
    public ResourceFieldMappingAudit getAuditWrapper() {
        return patientBuilder.getAuditWrapper();
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
}
