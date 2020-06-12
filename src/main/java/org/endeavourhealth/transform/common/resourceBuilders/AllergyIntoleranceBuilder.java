package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.common.fhir.AnnotationHelper;
import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

import java.util.Date;

public class AllergyIntoleranceBuilder extends ResourceBuilderBase
                                        implements HasCodeableConceptI {

    private AllergyIntolerance allergyIntolerance = null;

    public AllergyIntoleranceBuilder() {
        this(null);
    }

    public AllergyIntoleranceBuilder(AllergyIntolerance allergyIntolerance) {
        this(allergyIntolerance, null);
    }

    public AllergyIntoleranceBuilder(AllergyIntolerance allergyIntolerance, ResourceFieldMappingAudit audit) {
        super(audit);

        this.allergyIntolerance = allergyIntolerance;
        if (this.allergyIntolerance == null) {
            this.allergyIntolerance = new AllergyIntolerance();
            this.allergyIntolerance.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_ALLERGY_INTOLERANCE));
        }
    }

    @Override
    public DomainResource getResource() {
        return allergyIntolerance;
    }

    public void setPatient(Reference patientReference, CsvCell... sourceCells) {
        this.allergyIntolerance.setPatient(patientReference);

        auditValue("patient.reference", sourceCells);
    }

    public void setClinician(Reference practitionerReference, CsvCell... sourceCells) {
        //note that the "recorder" field is actually used to store the named clinician,
        //and the standard "recorded by" extension is used to store who physically entered it into the source software
        this.allergyIntolerance.setRecorder(practitionerReference);

        auditValue("recorder.reference", sourceCells);
    }

    public void setRecordedDate(Date enteredDateTime, CsvCell... sourceCells) {
        this.allergyIntolerance.setRecordedDate(enteredDateTime);

        auditValue("recordedDate", sourceCells);
    }

    public void setOnsetDate(DateTimeType dateTimeType, CsvCell... sourceCells) {
        this.allergyIntolerance.setOnsetElement(dateTimeType);

        auditValue("onset", sourceCells);
    }

    public void setEndDate(Date d, CsvCell... sourceCells) {
        if (d == null) {
            this.allergyIntolerance.setLastOccurence(null);

        } else {
            this.allergyIntolerance.setLastOccurence(d);
            auditValue("lastOccurence", sourceCells);
        }
    }

    public void setSeverity(AllergyIntolerance.AllergyIntoleranceSeverity allergyIntoleranceSeverity, CsvCell... sourceCells) {

        if (this.allergyIntolerance.hasReaction()) {
            this.allergyIntolerance.getReaction().get(0).setSeverity(allergyIntoleranceSeverity);
        } else {
            this.allergyIntolerance.addReaction().setSeverity(allergyIntoleranceSeverity);
        }

        auditValue("severity", sourceCells);
    }

    public void setCertainty(AllergyIntolerance.AllergyIntoleranceCertainty allergyIntoleranceCertainty, CsvCell... sourceCells) {

        if (this.allergyIntolerance.hasReaction()) {
            this.allergyIntolerance.getReaction().get(0).setCertainty(allergyIntoleranceCertainty);
        } else {
            this.allergyIntolerance.addReaction().setCertainty(allergyIntoleranceCertainty);
        }

        auditValue("certainty", sourceCells);
    }

    public void setCategory(AllergyIntolerance.AllergyIntoleranceCategory allergyIntoleranceCategory, CsvCell... sourceCells) {
        this.allergyIntolerance.setCategory(allergyIntoleranceCategory);

        auditValue("category", sourceCells);
    }

    public void setStatus(AllergyIntolerance.AllergyIntoleranceStatus allergyIntoleranceStatus, CsvCell... sourceCells) {
        this.allergyIntolerance.setStatus(allergyIntoleranceStatus);

        auditValue("status", sourceCells);
    }

    public void setNote(String text, CsvCell... sourceCells) {
        Annotation annotation = AnnotationHelper.createAnnotation(text);
        this.allergyIntolerance.setNote(annotation);

        auditValue("note.text", sourceCells);
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

    public void setEncounter(Reference reference, CsvCell... sourceCells) {
        createOrUpdateEncounterExtension(reference, sourceCells);
    }

    public void setRecordedBy(Reference practitionerReference, CsvCell... sourceCells) {
        createOrUpdateRecordedByExtension(practitionerReference, sourceCells);
    }

    public void setParentResource(Reference reference, CsvCell... sourceCells) {
        super.createOrUpdateParentResourceExtension(reference, sourceCells);
    }

    @Override
    public CodeableConcept createNewCodeableConcept(CodeableConceptBuilder.Tag tag, boolean useExisting) {
        if (tag == CodeableConceptBuilder.Tag.Allergy_Intolerance_Main_Code) {

            if (this.allergyIntolerance.hasSubstance()) {
                if (useExisting) {
                    return allergyIntolerance.getSubstance();
                } else {
                    throw new IllegalArgumentException("Trying to add new code to AllergyIntolerance that already has one");
                }
            }
            this.allergyIntolerance.setSubstance(new CodeableConcept());
            return this.allergyIntolerance.getSubstance();

        } else {
            throw new IllegalArgumentException("Invalid tag [" + tag + "]");
        }
    }

    @Override
    public String getCodeableConceptJsonPath(CodeableConceptBuilder.Tag tag, CodeableConcept codeableConcept) {
        if (tag == CodeableConceptBuilder.Tag.Allergy_Intolerance_Main_Code) {
            return "substance";

        } else {
            throw new IllegalArgumentException("Invalid tag [" + tag + "]");
        }
    }

    @Override
    public void removeCodeableConcept(CodeableConceptBuilder.Tag tag, CodeableConcept codeableConcept) {
        if (tag == CodeableConceptBuilder.Tag.Allergy_Intolerance_Main_Code) {
            this.allergyIntolerance.setSubstance(null);

        } else {
            throw new IllegalArgumentException("Invalid tag [" + tag + "]");
        }
    }
}
