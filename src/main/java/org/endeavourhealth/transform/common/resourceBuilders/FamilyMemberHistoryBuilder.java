package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.FamilyMember;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

import java.util.Date;

public class FamilyMemberHistoryBuilder extends ResourceBuilderBase
                                        implements HasCodeableConceptI {

    private FamilyMemberHistory familyMemberHistory = null;

    public FamilyMemberHistoryBuilder() {
        this(null);
    }

    public FamilyMemberHistoryBuilder(FamilyMemberHistory familyMemberHistory) {
        this.familyMemberHistory = familyMemberHistory;
        if (this.familyMemberHistory == null) {
            this.familyMemberHistory = new FamilyMemberHistory();
            this.familyMemberHistory.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_FAMILY_MEMBER_HISTORY));
        }
    }

    @Override
    public DomainResource getResource() {
        return familyMemberHistory;
    }

    public void setPatient(Reference patientReference, CsvCell... sourceCells) {
        this.familyMemberHistory.setPatient(patientReference);

        auditValue("patient.reference", sourceCells);
    }

    public void setDate(DateTimeType dateTimeType, CsvCell... sourceCells) {
        this.familyMemberHistory.setDateElement(dateTimeType);

        auditValue("dateValue", sourceCells);
    }

    public void setStatus(FamilyMemberHistory.FamilyHistoryStatus status, CsvCell... sourceCells) {
        this.familyMemberHistory.setStatus(status);

        auditValue("status", sourceCells);
    }

    public void setRelationship(FamilyMember familyMember, CsvCell... sourceCells) {
        CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(familyMember);
        this.familyMemberHistory.setRelationship(codeableConcept);

        auditValue("relationship.coding[0]", sourceCells);
    }

    public void setClinician(Reference practitionerReference, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateExtension(this.familyMemberHistory, FhirExtensionUri.FAMILY_MEMBER_HISTORY_REPORTED_BY, practitionerReference);

        auditReferenceExtension(extension, sourceCells);
    }


    public void setEncounter(Reference encounterReference, CsvCell... sourceCells) {
        createOrUpdateEncounterExtension(encounterReference, sourceCells);
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

    public void setNotes(String notes, CsvCell... sourceCells) {
        Annotation annotation = AnnotationHelper.createAnnotation(notes);
        this.familyMemberHistory.setNote(annotation);

        auditValue("note.text", sourceCells);
    }

    private FamilyMemberHistory.FamilyMemberHistoryConditionComponent findOrCreateCondition() {
        if (this.familyMemberHistory.hasCondition()) {
            return this.familyMemberHistory.getCondition().get(0);
        } else {
            return this.familyMemberHistory.addCondition();
        }
    }

    @Override
    public CodeableConcept getOrCreateCodeableConcept(String tag) {
        FamilyMemberHistory.FamilyMemberHistoryConditionComponent condition = findOrCreateCondition();
        if (condition.hasCode()) {
            return condition.getCode();
        } else {
            CodeableConcept codeableConcept = new CodeableConcept();
            condition.setCode(codeableConcept);
            return codeableConcept;
        }
    }

    @Override
    public String getCodeableConceptJsonPath(String tag) {
        return "condition[0].code";
    }
}
