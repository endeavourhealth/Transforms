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
