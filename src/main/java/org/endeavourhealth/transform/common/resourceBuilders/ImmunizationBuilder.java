package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.common.fhir.AnnotationHelper;
import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

import java.util.Date;

public class ImmunizationBuilder extends ResourceBuilderBase
                                 implements HasCodeableConceptI {

    private Immunization immunization = null;

    public ImmunizationBuilder() {
        this(null);
    }

    public ImmunizationBuilder(Immunization immunization) {
        this.immunization = immunization;
        if (this.immunization == null) {
            this.immunization = new Immunization();
            this.immunization.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_IMMUNIZATION));
        }
    }

    @Override
    public DomainResource getResource() {
        return immunization;
    }

    public void setPatient(Reference patientReference, CsvCell... sourceCells) {
        this.immunization.setPatient(patientReference);

        auditValue("patient.reference", sourceCells);
    }

    public void setStatus(String status, CsvCell... sourceCells) {
        this.immunization.setStatus(status);

        auditValue("status", sourceCells);
    }

    public void setWasNotGiven(boolean notGiven, CsvCell... sourceCells) {
        this.immunization.setWasNotGiven(notGiven);

        auditValue("wasNotGiven", sourceCells);
    }

    public void setReported(boolean reported, CsvCell... sourceCells) {
        this.immunization.setReported(reported);

        auditValue("reported", sourceCells);
    }

    public void setPerformedDate(DateTimeType dateTimeType, CsvCell... sourceCells) {
        this.immunization.setDateElement(dateTimeType);

        auditValue("date", sourceCells);
    }

    public void setPerformer(Reference practitionerReference, CsvCell... sourceCells) {
        this.immunization.setPerformer(practitionerReference);

        auditValue("performer.reference", sourceCells);
    }

    public void setEncounter(Reference encounterReference, CsvCell... sourceCells) {
        this.immunization.setEncounter(encounterReference);

        auditValue("encounter.reference", sourceCells);
    }

    public void setNote(String notes, CsvCell... sourceCells) {

        //we only support a single note, so just replace the existing one
        if (this.immunization.hasNote()) {
            this.immunization.getNote().clear();
        }

        Annotation annotation = AnnotationHelper.createAnnotation(notes);
        this.immunization.addNote(annotation);

        auditValue("note[0].text", sourceCells);
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

    @Override
    public CodeableConcept createNewCodeableConcept(String tag) {
        if (this.immunization.hasVaccineCode()) {
            throw new IllegalArgumentException("Trying to add new code to Immunization when it already has one");
        }

        CodeableConcept codeableConcept = new CodeableConcept();
        this.immunization.setVaccineCode(codeableConcept);
        return codeableConcept;
    }

    @Override
    public String getCodeableConceptJsonPath(String tag, CodeableConcept codeableConcept) {
        return "vaccineCode";
    }

    @Override
    public void removeCodeableConcepts(String tag) {
        this.immunization.setVaccineCode(null);
    }
}
