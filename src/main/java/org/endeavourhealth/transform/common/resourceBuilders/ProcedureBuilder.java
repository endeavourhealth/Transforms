package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.common.fhir.AnnotationHelper;
import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

import java.util.Date;
import java.util.List;

public class ProcedureBuilder extends ResourceBuilderBase
                             implements HasCodeableConceptI, HasIdentifierI {

    private Procedure procedure = null;

    public static final String TAG_CODEABLE_CONCEPT_CODE = "Code";
    public static final String TAG_CODEABLE_CONCEPT_CATEGORY = "Category";


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

    public void setParentResource(Reference reference, CsvCell... sourceCells) {
        super.createOrUpdateParentResourceExtension(reference, sourceCells);
    }

    /*public void addIdentifier(Identifier identifier, CsvCell... sourceCells) {
        this.procedure.addIdentifier(identifier);

        int index = this.procedure.getIdentifier().size()-1;
        auditValue("identifier[" + index + "].value", sourceCells);
    }*/


    @Override
    public CodeableConcept createNewCodeableConcept(String tag) {
        if (tag.equals(TAG_CODEABLE_CONCEPT_CODE)) {
            if (this.procedure.hasCode()) {
                throw new IllegalArgumentException("Trying to add code to Procedure that already has one");
            }
            this.procedure.setCode(new CodeableConcept());
            return this.procedure.getCode();

        } else if (tag.equals(TAG_CODEABLE_CONCEPT_CATEGORY)) {
            if (this.procedure.hasCategory()) {
                throw new IllegalArgumentException("Trying to add category to Procedure that already has one");
            }
            this.procedure.setCategory(new CodeableConcept());
            return this.procedure.getCategory();

        } else {
            throw new IllegalArgumentException("Invalid tag [" + tag + "]");
        }
    }

    @Override
    public String getCodeableConceptJsonPath(String tag, CodeableConcept codeableConcept) {
        if (tag.equals(TAG_CODEABLE_CONCEPT_CODE)) {
            return "code";

        } else if (tag.equals(TAG_CODEABLE_CONCEPT_CATEGORY)) {
            return "category";

        } else {
            throw new IllegalArgumentException("Invalid tag [" + tag + "]");
        }
    }

    @Override
    public void removeCodeableConcepts(String tag) {
        if (tag.equals(TAG_CODEABLE_CONCEPT_CODE)) {
            this.procedure.setCode(null);

        } else if (tag.equals(TAG_CODEABLE_CONCEPT_CATEGORY)) {
            this.procedure.setCategory(null);

        } else {
            throw new IllegalArgumentException("Invalid tag [" + tag + "]");
        }
    }

    @Override
    public Identifier addIdentifier() {
        return this.procedure.addIdentifier();
    }

    @Override
    public String getIdentifierJsonPrefix(Identifier identifier) {
        int index = this.procedure.getIdentifier().indexOf(identifier);
        return "identifier[" + index + "]";
    }

    @Override
    public List<Identifier> getIdentifiers() {
        return this.procedure.getIdentifier();
    }

    @Override
    public void removeIdentifier(Identifier identifier) {
        this.procedure.getIdentifier().remove(identifier);
    }
}
