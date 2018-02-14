package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

import java.util.Date;

public class DiagnosticOrderBuilder extends ResourceBuilderBase
                                    implements HasCodeableConceptI {

    private DiagnosticOrder diagnosticOrder = null;

    public DiagnosticOrderBuilder() {
        this(null);
    }

    public DiagnosticOrderBuilder(DiagnosticOrder diagnosticOrder) {
        this.diagnosticOrder = diagnosticOrder;
        if (this.diagnosticOrder == null) {
            this.diagnosticOrder = new DiagnosticOrder();
            this.diagnosticOrder.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_DIAGNOSTIC_ORDER));
        }
    }

    @Override
    public DomainResource getResource() {
        return diagnosticOrder;
    }

    public void setPatient(Reference patientReference, CsvCell... sourceCells) {
        this.diagnosticOrder.setSubject(patientReference);

        auditValue("patient.reference", sourceCells);
    }

    public void setOrderedBy(Reference practitionerReference, CsvCell... sourceCells) {
        this.diagnosticOrder.setOrderer(practitionerReference);

        auditValue("orderer.reference", sourceCells);
    }

    public void setEncounter(Reference encounterReference, CsvCell... sourceCells) {
        this.diagnosticOrder.setEncounter(encounterReference);

        auditValue("encounter.reference", sourceCells);
    }

    private DiagnosticOrder.DiagnosticOrderItemComponent getOrderItemElement() {
        DiagnosticOrder.DiagnosticOrderItemComponent diagnosticOrderItemComponent = null;
        if (this.diagnosticOrder.hasItem()) {
            diagnosticOrderItemComponent = this.diagnosticOrder.getItem().get(0);
        } else {
            diagnosticOrderItemComponent = this.diagnosticOrder.addItem();
        }
        return diagnosticOrderItemComponent;
    }

    public void setNote(String note, CsvCell... sourceCells) {
        Annotation annotation = null;
        if (this.diagnosticOrder.hasNote()) {
            annotation = this.diagnosticOrder.getNote().get(0);
        } else {
            annotation = this.diagnosticOrder.addNote();
        }
        annotation.setText(note);

        auditValue("note[0].text", sourceCells);
    }

    private DiagnosticOrder.DiagnosticOrderEventComponent getOrderEventComponent() {
        if (this.diagnosticOrder.hasEvent()) {
            return this.diagnosticOrder.getEvent().get(0);
        } else {
            return this.diagnosticOrder.addEvent();
        }
    }

    public void setDateTime(DateTimeType dateTimeType, CsvCell... sourceCells) {
        getOrderEventComponent().setDateTimeElement(dateTimeType);

        auditValue("event[0].valueDateTime", sourceCells);
    }

    public void setRecordedDate(Date recordedDate, CsvCell... sourceCells) {
        createOrUpdateRecordedDateExtension(recordedDate, sourceCells);
    }

    public void setRecordedBy(Reference practitionerReference, CsvCell... sourceCells) {
        createOrUpdateRecordedByExtension(practitionerReference, sourceCells);
    }

    public void addDocumentIdentifier(Identifier fhirIdentifier, CsvCell... sourceCells) {
        createOrUpdateDocumentIdExtension(fhirIdentifier, sourceCells);
    }

    public void setIsReview(boolean b, CsvCell... sourceCells) {
        createOrUpdateIsReviewExtension(b, sourceCells);
    }

    public void setIsConfidential(boolean b, CsvCell... sourceCells) {
        createOrUpdateIsConfidentialExtension(b, sourceCells);
    }

    @Override
    public CodeableConcept getOrCreateCodeableConcept(String tag) {
        DiagnosticOrder.DiagnosticOrderItemComponent item = getOrderItemElement();
        if (!item.hasCode()) {
            item.setCode(new CodeableConcept());
        }
        return item.getCode();
    }

    @Override
    public String getCodeableConceptJsonPath(String tag) {
        return "item[0].code";
    }
}
