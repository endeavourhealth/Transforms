package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
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
        this(diagnosticOrder, null);
    }

    public DiagnosticOrderBuilder(DiagnosticOrder diagnosticOrder, ResourceFieldMappingAudit audit) {
        super(audit);

        this.diagnosticOrder = diagnosticOrder;
        if (this.diagnosticOrder == null) {
            this.diagnosticOrder = new DiagnosticOrder();
            this.diagnosticOrder.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_DIAGNOSTIC_ORDER));
        }
    }

    @Override
    public DomainResource getResource() {
        return diagnosticOrder;
    }

    public void setPatient(Reference patientReference, CsvCell... sourceCells) {
        this.diagnosticOrder.setSubject(patientReference);

        auditValue("subject.reference", sourceCells);
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

        auditValue("event[0].dateTime", sourceCells);
    }

    public void setStatus(DiagnosticOrder.DiagnosticOrderStatus status, CsvCell... sourceCells) {
        getOrderEventComponent().setStatus(status);

        auditValue("event[0].status", sourceCells);
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

    public void setParentResource(Reference reference, CsvCell... sourceCells) {
        super.createOrUpdateParentResourceExtension(reference, sourceCells);
    }

    @Override
    public CodeableConcept createNewCodeableConcept(CodeableConceptBuilder.Tag tag, boolean useExisting) {

        if (tag == CodeableConceptBuilder.Tag.Diagnostic_Order_Main_Code) {
            DiagnosticOrder.DiagnosticOrderItemComponent item = getOrderItemElement();
            if (item.hasCode()) {
                if (useExisting) {
                    return item.getCode();
                } else {
                    throw new IllegalArgumentException("Trying to add new code to DiagnosticOrder item when it already has one");
                }
            }
            item.setCode(new CodeableConcept());
            return item.getCode();

        } else {
            throw new IllegalArgumentException("Unknown tag [" + tag + "]");
        }
    }

    @Override
    public String getCodeableConceptJsonPath(CodeableConceptBuilder.Tag tag, CodeableConcept codeableConcept) {
        if (tag == CodeableConceptBuilder.Tag.Diagnostic_Order_Main_Code) {
            return "item[0].code";

        } else {
            throw new IllegalArgumentException("Unknown tag [" + tag + "]");
        }
    }

    @Override
    public void removeCodeableConcept(CodeableConceptBuilder.Tag tag, CodeableConcept codeableConcept) {
        if (tag == CodeableConceptBuilder.Tag.Diagnostic_Order_Main_Code) {
            DiagnosticOrder.DiagnosticOrderItemComponent item = getOrderItemElement();
            item.setCode(null);

        } else {
            throw new IllegalArgumentException("Unknown tag [" + tag + "]");
        }
    }
}
