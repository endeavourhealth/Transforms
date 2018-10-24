package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.FamilyMember;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

import java.util.Date;
import java.util.List;

public class FamilyMemberHistoryBuilder extends ResourceBuilderBase
        implements HasCodeableConceptI, HasIdentifierI {

    private FamilyMemberHistory familyMemberHistory = null;

    public FamilyMemberHistoryBuilder() {
        this(null);
    }

    public FamilyMemberHistoryBuilder(FamilyMemberHistory familyMemberHistory) {
        this(familyMemberHistory, null);
    }

    public FamilyMemberHistoryBuilder(FamilyMemberHistory familyMemberHistory, ResourceFieldMappingAudit audit) {
        super(audit);

        this.familyMemberHistory = familyMemberHistory;
        if (this.familyMemberHistory == null) {
            this.familyMemberHistory = new FamilyMemberHistory();
            this.familyMemberHistory.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_FAMILY_MEMBER_HISTORY));
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

        auditValue("date", sourceCells);
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

    public void setRelationshipFreeText(String typeDesc, CsvCell... sourceCells) {
        CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(typeDesc);
        this.familyMemberHistory.setRelationship(codeableConcept);

        auditValue("relationship.coding[0].text", sourceCells);
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


    public void setParentResource(Reference reference, CsvCell... sourceCells) {
        super.createOrUpdateParentResourceExtension(reference, sourceCells);
    }

    @Override
    public CodeableConcept createNewCodeableConcept(CodeableConceptBuilder.Tag tag, boolean useExisting) {

        if (tag == CodeableConceptBuilder.Tag.Family_Member_History_Main_Code) {

            FamilyMemberHistory.FamilyMemberHistoryConditionComponent condition = findOrCreateCondition();
            if (condition.hasCode()) {
                if (useExisting) {
                    return condition.getCode();
                } else {
                    throw new IllegalArgumentException("Trying to add new code to FamilyMemberHistory when it already has one");
                }
            }
            CodeableConcept codeableConcept = new CodeableConcept();
            condition.setCode(codeableConcept);
            return codeableConcept;

        } else {
            throw new IllegalArgumentException("Unknown tag [" + tag + "]");
        }
    }

    @Override
    public String getCodeableConceptJsonPath(CodeableConceptBuilder.Tag tag, CodeableConcept codeableConcept) {
        if (tag == CodeableConceptBuilder.Tag.Family_Member_History_Main_Code) {

            return "condition[0].code";

        } else {
            throw new IllegalArgumentException("Unknown tag [" + tag + "]");
        }
    }

    @Override
    public void removeCodeableConcept(CodeableConceptBuilder.Tag tag, CodeableConcept codeableConcept) {
        if (tag == CodeableConceptBuilder.Tag.Family_Member_History_Main_Code) {

            FamilyMemberHistory.FamilyMemberHistoryConditionComponent condition = findOrCreateCondition();
            condition.setCode(null);

        } else {
            throw new IllegalArgumentException("Unknown tag [" + tag + "]");
        }
    }

    public void setEndDate(DateTimeType dateTimeType, CsvCell... sourceCells) {

        //confusingly, the START DATE isn't stored in the condition period, but stored in the date element.
        //This is slightly wrong, since date is supposed to be RECORDED date, but the date is already like that
        FamilyMemberHistory.FamilyMemberHistoryConditionComponent condition = findOrCreateCondition();
        Period period = null;
        if (condition.hasOnset()) {
            Type type = condition.getOnset();
            if (type instanceof Period) {
                period = (Period) type;

            } else {
                throw new RuntimeException("Cannot set end date because onset object is already set to a " + type.getClass());
            }
        } else {
            period = new Period();
            condition.setOnset(period);
        }

        if (dateTimeType == null) {
            period.setEndElement(null);
        } else {
            period.setEndElement(dateTimeType);
        }

        auditValue("condition[0].onsetPeriod.end", sourceCells);
    }

    @Override
    public Identifier addIdentifier() {
        return this.familyMemberHistory.addIdentifier();
    }

    @Override
    public String getIdentifierJsonPrefix(Identifier identifier) {
        int index = this.familyMemberHistory.getIdentifier().indexOf(identifier);
        return "identifier[" + index + "]";
    }

    @Override
    public List<Identifier> getIdentifiers() {
        return this.familyMemberHistory.getIdentifier();
    }

    @Override
    public void removeIdentifier(Identifier identifier) {
        this.familyMemberHistory.getIdentifier().remove(identifier);
    }
}
