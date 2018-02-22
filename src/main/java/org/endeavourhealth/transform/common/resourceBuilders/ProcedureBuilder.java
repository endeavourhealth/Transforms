package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.common.fhir.AnnotationHelper;
import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

import java.util.Date;

public class ProcedureBuilder extends ResourceBuilderBase
                             implements HasCodeableConceptI {

    private Procedure procedure = null;

    public ProcedureBuilder() {
        this(null);
    }

    public ProcedureBuilder(Procedure procedure) {
        this.procedure = procedure;
        if (this.procedure == null) {
            this.procedure = new Procedure();
            this.procedure.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_PROCEDURE));
        }
    }

    @Override
    public DomainResource getResource() {
        return procedure;
    }



    public void setPatient(Reference referenceValue, CsvCell... sourceCells) {
        this.procedure.setSubject(referenceValue);

        auditValue("subject.reference", sourceCells);
    }

    public void setStatus(Procedure.ProcedureStatus status, CsvCell... sourceCells) {
        this.procedure.setStatus(status);

        auditValue("status", sourceCells);
    }

    public void setPerformed(DateTimeType dateTimeType, CsvCell... sourceCells) {
        this.procedure.setPerformed(dateTimeType);

        auditValue("performedDateTime", sourceCells);
    }

    public void addPerformer(Reference practitionerReference, CsvCell... sourceCells) {
        Procedure.ProcedurePerformerComponent fhirPerformer = this.procedure.addPerformer();
        fhirPerformer.setActor(practitionerReference);

        int index = this.procedure.getPerformer().size()-1;
        auditValue("performer[" + index + "].actor.reference", sourceCells);
    }

    public void addNotes(String notes, CsvCell... sourceCells) {
        Annotation annotation = AnnotationHelper.createAnnotation(notes);
        this.procedure.addNotes(annotation);

        int index = this.procedure.getNotes().size()-1;
        auditValue("notes[" + index + "].text", sourceCells);
    }

    public void setEncounter(Reference encounterReference, CsvCell... sourceCells) {
        this.procedure.setEncounter(encounterReference);

        auditValue("encounter.reference", sourceCells);
    }

    public void setRecordedBy(Reference practitionerReference, CsvCell... sourceCells) {
        createOrUpdateRecordedByExtension(practitionerReference, sourceCells);
    }

    public void setRecordedDate(Date recordedDate, CsvCell... sourceCells) {
        createOrUpdateRecordedDateExtension(recordedDate, sourceCells);
    }

    public void addDocumentIdentifier(Identifier identifier, CsvCell... sourceCells) {
        createOrUpdateDocumentIdExtension(identifier, sourceCells);
    }

    public void setIsReview(boolean isReview, CsvCell... sourceCells) {
        createOrUpdateIsReviewExtension(isReview, sourceCells);
    }

    public void setIsConfidential(boolean isConfidential, CsvCell... sourceCells) {
        createOrUpdateIsConfidentialExtension(isConfidential, sourceCells);
    }

    @Override
    public CodeableConcept getOrCreateCodeableConcept(String tag) {
        if (!this.procedure.hasCode()) {
            this.procedure.setCode(new CodeableConcept());
        }
        return this.procedure.getCode();
    }

    @Override
    public String getCodeableConceptJsonPath(String tag) {
        return "code";
    }

    public void setParentResource(Reference reference, CsvCell... sourceCells) {
        super.createOrUpdateParentResourceExtension(reference, sourceCells);
    }
}
