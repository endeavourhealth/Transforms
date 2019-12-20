package org.endeavourhealth.transform.common.resourceBuilders;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.common.fhir.schema.ProblemRelationshipType;
import org.endeavourhealth.common.fhir.schema.ProblemSignificance;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class ConditionBuilder extends ResourceBuilderBase
                              implements HasCodeableConceptI,
                                        HasContainedListI, HasIdentifierI {

    private static final Logger LOG = LoggerFactory.getLogger(ConditionBuilder.class);

    private Condition condition = null;

    public ConditionBuilder() {
        this(null);
    }

    public ConditionBuilder(Condition condition) {
        this(condition, null);
    }

    public ConditionBuilder(Condition condition, ResourceFieldMappingAudit auditWrapper) {
        super(auditWrapper);

        this.condition = condition;
        if (this.condition == null) {
            this.condition = new Condition();
            //we use two different profile URLs for this resource, so should call
            //setAsProblem(..) to explicitly set it, but default to the most common
            setAsProblem(true);
        }
    }

    @Override
    public DomainResource getResource() {
        return condition;
    }

    @Override
    public String getContainedListExtensionUrl() {
        return FhirExtensionUri.PROBLEM_ASSOCIATED_RESOURCE;
    }

    public void setAsProblem(boolean isProblem) {
        if (isProblem) {
            this.condition.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_PROBLEM));
        } else {
            this.condition.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_CONDITION));
        }
    }

    public boolean isProblem() {
        if (!condition.hasMeta()) {
            throw new IllegalArgumentException("Condition doesn't have meta element");
        }
        Meta meta = condition.getMeta();
        if (!meta.hasProfile()) {
            throw new IllegalAccessError("Meta element has no profile URL set");
        }
        UriType profile = meta.getProfile().get(0);
        return profile.getValue().equals(FhirProfileUri.PROFILE_URI_PROBLEM);
    }

    public void setPatient(Reference patientReference, CsvCell... sourceCells) {
        this.condition.setPatient(patientReference);

        auditValue("patient.reference", sourceCells);
    }

    public void setContext(String context, CsvCell... sourceCells) {
        if (Strings.isNullOrEmpty(context)) {
            ExtensionConverter.removeExtension(this.condition, FhirExtensionUri.RESOURCE_CONTEXT);

        } else {
            Extension extension = ExtensionConverter.createOrUpdateStringExtension(this.condition, FhirExtensionUri.RESOURCE_CONTEXT, context);

            auditStringExtension(extension, sourceCells);
        }
    }

    /*public void addExtension(Extension extension, CsvCell... sourceCells) {
        this.condition.addExtension(extension);

        auditValue("extension[" + this.condition.getExtension().size() + "]", sourceCells);
    }*/

    public void setClinician(Reference practitionerReference, CsvCell... sourceCells) {
        this.condition.setAsserter(practitionerReference);

        auditValue("asserter.reference", sourceCells);
    }

    public void setRecordedDate(Date enteredDateTime, CsvCell... sourceCells) {
        this.condition.setDateRecorded(enteredDateTime);

        auditValue("dateRecorded", sourceCells);
    }


    public void setEncounter(Reference encounterReference, CsvCell... sourceCells) {
        this.condition.setEncounter(encounterReference);

        auditValue("encounter.reference", sourceCells);
    }

    public void setCode(CodeableConcept code, CsvCell... sourceCells) {
        this.condition.setCode(code);

        auditValue("code", sourceCells);
    }

    public void setPartOfProblem(Reference problemReference, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateExtension(this.condition, FhirExtensionUri.CONDITION_PART_OF_PROBLEM, problemReference);

        auditReferenceExtension(extension, sourceCells);
    }


    public void setRecordedBy(Reference practitionerReference, CsvCell... sourceCells) {
        createOrUpdateRecordedByExtension(practitionerReference, sourceCells);
    }

    public void addDocumentIdentifier(Identifier identifier, CsvCell... sourceCells) {
        createOrUpdateDocumentIdExtension(identifier, sourceCells);
    }

    public void setIsReview(boolean isReview, CsvCell... sourceCells) {
        createOrUpdateIsReviewExtension(isReview, sourceCells);
    }

    public void setEpisodicity(String episodicity, CsvCell... sourceCells) {
        if (Strings.isNullOrEmpty(episodicity)) {
            ExtensionConverter.removeExtension(this.condition, FhirExtensionUri.PROBLEM_EPISODICITY);

        } else {
            Extension extension = ExtensionConverter.createOrUpdateStringExtension(this.condition, FhirExtensionUri.PROBLEM_EPISODICITY, episodicity);

            auditStringExtension(extension, sourceCells);
        }
    }

    public void setIsConfidential(boolean isConfidential, CsvCell... sourceCells) {
        createOrUpdateIsConfidentialExtension(isConfidential, sourceCells);
    }

    public void setIsPrimary(boolean isPrimary, CsvCell... sourceCells) {
        super.createOrUpdateIsPrimaryExtension(isPrimary, sourceCells);
    }

    public void setOnset(DateTimeType dateTimeType, CsvCell... sourceCells) {
        this.condition.setOnset(dateTimeType);

        auditValue("onsetDateTime", sourceCells);
    }

    public void setNotes(String notes, CsvCell... sourceCells) {
        if (Strings.isNullOrEmpty(notes)) {
            this.condition.setNotes(null);

        } else {
            this.condition.setNotes(notes);

            auditValue("notes", sourceCells);
        }
    }

    public String getNotes() {
        if (this.condition.hasNotes()) {
            return this.condition.getNotes();
        }
        return null;
    }

    /**
     * removed - it's a confusing extension added for data we never receive anyway
     */
    /*public void setAdditionalNotes(String additionalNodes, CsvCell... sourceCells) {
        if (Strings.isNullOrEmpty(additionalNodes)) {
            ExtensionConverter.removeExtension(this.condition, FhirExtensionUri.PROBLEM_ADDITIONAL_NOTES);

        } else {
            Extension extension = ExtensionConverter.createOrUpdateStringExtension(this.condition, FhirExtensionUri.PROBLEM_ADDITIONAL_NOTES, additionalNodes);

            auditStringExtension(extension, sourceCells);
        }
    }

    public String getAdditionalNotes() {
        StringType stringTypeType = (StringType)ExtensionConverter.findExtensionValue(this.condition, FhirExtensionUri.PROBLEM_ADDITIONAL_NOTES);
        if (stringTypeType != null) {
            return stringTypeType.getValue();
        }
        return null;
    }*/

    public void setCategory(String category, CsvCell... sourceCells) {
        if (Strings.isNullOrEmpty(category)) {
            this.condition.setCategory(null);

        } else {
            CodeableConcept codeableConcept = new CodeableConcept();
            Coding coding = codeableConcept.addCoding();
            coding.setSystem(FhirValueSetUri.VALUE_SET_CONDITION_CATEGORY);
            coding.setCode(category);
            this.condition.setCategory(codeableConcept);

            auditValue("category.coding[0].code", sourceCells);
        }
    }

    public void setEndDateOrBoolean(Type type, CsvCell... sourceCells) {
        if (type == null) {
            this.condition.setAbatement(null);

        } else {
            if (!(type instanceof DateType)
                    && !(type instanceof BooleanType)) {
                throw new IllegalArgumentException("Only DateType or BooleanType are supported for abatement");
            }

            this.condition.setAbatement(type);
            if (type instanceof DateType) {
                auditValue("abatementDate", sourceCells);

            } else {
                auditValue("abatementBoolean", sourceCells);
            }
        }
    }

    public Type getEndDateOrBoolean() {
        if (this.condition.hasAbatement()) {
            return this.condition.getAbatement();
        }
        return null;
    }

    public void setExpectedDuration(Integer days, CsvCell... sourceCells) {
        if (days == null) {
            ExtensionConverter.removeExtension(this.condition, FhirExtensionUri.PROBLEM_EXPECTED_DURATION);

        } else {
            Extension extension = ExtensionConverter.createOrUpdateIntegerExtension(this.condition, FhirExtensionUri.PROBLEM_EXPECTED_DURATION, days);
            auditIntegerExtension(extension, sourceCells);
        }
    }

    public Integer getExpectedDuration() {
        IntegerType integerType = (IntegerType)ExtensionConverter.findExtensionValue(this.condition, FhirExtensionUri.PROBLEM_EXPECTED_DURATION);
        if (integerType != null) {
            return integerType.getValue();
        }
        return null;
    }


    @Override
    public CodeableConcept createNewCodeableConcept(CodeableConceptBuilder.Tag tag, boolean useExisting) {
        if (tag == CodeableConceptBuilder.Tag.Condition_Main_Code) {
            if (this.condition.hasCode()) {
                if (useExisting) {
                    return condition.getCode();
                } else {
                    throw new IllegalArgumentException("Trying to add new code to Condition that already has one");
                }
            }
            this.condition.setCode(new CodeableConcept());
            return this.condition.getCode();

        } else {
            throw new IllegalArgumentException("Invalid tag [" + tag + "]");
        }
    }

    @Override
    public String getCodeableConceptJsonPath(CodeableConceptBuilder.Tag tag, CodeableConcept codeableConcept) {
        if (tag == CodeableConceptBuilder.Tag.Condition_Main_Code) {
            return "code";

        } else {
            throw new IllegalArgumentException("Invalid tag [" + tag + "]");
        }
    }

    @Override
    public void removeCodeableConcept(CodeableConceptBuilder.Tag tag, CodeableConcept codeableConcept) {

        if (tag == CodeableConceptBuilder.Tag.Condition_Main_Code) {
            this.condition.setCode(null);

        } else {
            throw new IllegalArgumentException("Invalid tag [" + tag + "]");
        }
    }


    public void setProblemLastReviewDate(DateType lastReviewDateType, CsvCell... sourceCells) {
        if (lastReviewDateType == null) {

            Extension outerExtension = ExtensionConverter.findExtension(this.condition, FhirExtensionUri.PROBLEM_LAST_REVIEWED);
            if (outerExtension != null) {
                //remove the sub-extension
                ExtensionConverter.removeExtension(outerExtension, FhirExtensionUri._PROBLEM_LAST_REVIEWED__DATE);

                //if this leaves the main extension empty, remove that too
                if (!outerExtension.hasExtension()) {
                    ExtensionConverter.removeExtension(this.condition, FhirExtensionUri.PROBLEM_LAST_REVIEWED);
                }
            }

        } else {

            //this is stored in a compound extension (extension with extensions)
            Extension outerExtension = ExtensionConverter.findOrCreateExtension(this.condition, FhirExtensionUri.PROBLEM_LAST_REVIEWED);
            Extension innerExtension = ExtensionConverter.createOrUpdateExtension(outerExtension, FhirExtensionUri._PROBLEM_LAST_REVIEWED__DATE, lastReviewDateType);

            int outerIndex = this.condition.getExtension().indexOf(outerExtension);
            int innerIndex = outerExtension.getExtension().indexOf(innerExtension);
            auditValue("extension[" + outerIndex + "].extension[" + innerIndex + "].valueDate", sourceCells);
        }
    }

    public DateType getProblemLastReviewDate() {
        Extension outerExtension = ExtensionConverter.findExtension(this.condition, FhirExtensionUri.PROBLEM_LAST_REVIEWED);
        if (outerExtension == null) {
            return null;
        }
        return (DateType)ExtensionConverter.findExtensionValue(outerExtension, FhirExtensionUri._PROBLEM_LAST_REVIEWED__DATE);
    }

    public void setProblemLastReviewedBy(Reference practitionerReference, CsvCell... sourceCells) {

        if (practitionerReference == null) {

            Extension outerExtension = ExtensionConverter.findExtension(this.condition, FhirExtensionUri.PROBLEM_LAST_REVIEWED);
            if (outerExtension != null) {
                //remove the sub-extension
                ExtensionConverter.removeExtension(outerExtension, FhirExtensionUri._PROBLEM_LAST_REVIEWED__PERFORMER);

                //if this leaves the main extension empty, remove that too
                if (!outerExtension.hasExtension()) {
                    ExtensionConverter.removeExtension(this.condition, FhirExtensionUri.PROBLEM_LAST_REVIEWED);
                }
            }

        } else {

            //this is stored in a compound extension (extension with extensions)
            Extension outerExtension = ExtensionConverter.findOrCreateExtension(this.condition, FhirExtensionUri.PROBLEM_LAST_REVIEWED);
            Extension innerExtension = ExtensionConverter.createOrUpdateExtension(outerExtension, FhirExtensionUri._PROBLEM_LAST_REVIEWED__PERFORMER, practitionerReference);

            int outerIndex = this.condition.getExtension().indexOf(outerExtension);
            int innerIndex = outerExtension.getExtension().indexOf(innerExtension);

            auditValue("extension[" + outerIndex + "].extension[" + innerIndex + "].valueReference.reference", sourceCells);
        }
    }

    public Reference getProblemLastReviewedBy() {
        Extension outerExtension = ExtensionConverter.findExtension(this.condition, FhirExtensionUri.PROBLEM_LAST_REVIEWED);
        if (outerExtension == null) {
            return null;
        }
        return (Reference)ExtensionConverter.findExtensionValue(outerExtension, FhirExtensionUri._PROBLEM_LAST_REVIEWED__PERFORMER);
    }

    public void setProblemSignificance(ProblemSignificance fhirSignificance, CsvCell... sourceCells) {

        if (fhirSignificance == null) {
            ExtensionConverter.removeExtension(this.condition, FhirExtensionUri.PROBLEM_SIGNIFICANCE);

        } else {
            CodeableConcept fhirConcept = CodeableConceptHelper.createCodeableConcept(fhirSignificance);
            Extension extension = ExtensionConverter.createOrUpdateExtension(this.condition, FhirExtensionUri.PROBLEM_SIGNIFICANCE, fhirConcept);

            auditCodeableConceptExtension(extension, sourceCells);
        }
    }

    public ProblemSignificance getProblemSignificance() {
        CodeableConcept codeableConcept = (CodeableConcept)ExtensionConverter.findExtensionValue(this.condition, FhirExtensionUri.PROBLEM_SIGNIFICANCE);
        return ProblemSignificance.fromCodeableConcept(codeableConcept);
    }

    public void setParentProblem(Reference problemReference, CsvCell... sourceCells) {

        if (problemReference == null) {
            Extension outerExtension = ExtensionConverter.findExtension(this.condition, FhirExtensionUri.PROBLEM_RELATED);
            if (outerExtension != null) {
                //remove the sub-extension
                ExtensionConverter.removeExtension(outerExtension, FhirExtensionUri._PROBLEM_RELATED__TARGET);

                //if this leaves the main extension empty, remove that too
                if (!outerExtension.hasExtension()) {
                    ExtensionConverter.removeExtension(this.condition, FhirExtensionUri.PROBLEM_RELATED);
                }
            }

        } else {

            //this is stored in a compound extension (extension with extensions)
            Extension outerExtension = ExtensionConverter.findOrCreateExtension(this.condition, FhirExtensionUri.PROBLEM_RELATED);
            Extension innerExtension = ExtensionConverter.createOrUpdateExtension(outerExtension, FhirExtensionUri._PROBLEM_RELATED__TARGET, problemReference);

            int outerIndex = this.condition.getExtension().indexOf(outerExtension);
            int innerIndex = outerExtension.getExtension().indexOf(innerExtension);
            auditValue("extension[" + outerIndex + "].extension[" + innerIndex + "].valueReference.reference", sourceCells);
        }
    }

    public Reference getParentProblem() {
        Extension outerExtension = ExtensionConverter.findExtension(this.condition, FhirExtensionUri.PROBLEM_RELATED);
        if (outerExtension == null) {
            return null;
        }
        return (Reference)ExtensionConverter.findExtensionValue(outerExtension, FhirExtensionUri._PROBLEM_RELATED__TARGET);
    }

    public void setParentProblemRelationship(ProblemRelationshipType fhirRelationshipType, CsvCell... sourceCells) {

        if (fhirRelationshipType == null) {
            Extension outerExtension = ExtensionConverter.findExtension(this.condition, FhirExtensionUri.PROBLEM_RELATED);
            if (outerExtension != null) {
                //remove the sub-extension
                ExtensionConverter.removeExtension(outerExtension, FhirExtensionUri._PROBLEM_RELATED__TYPE);

                //if this leaves the main extension empty, remove that too
                if (!outerExtension.hasExtension()) {
                    ExtensionConverter.removeExtension(this.condition, FhirExtensionUri.PROBLEM_RELATED);
                }
            }

        } else {

            //not sure why this is stored in a string type, but it's been like that for a long time, so don't change
            StringType stringType = new StringType(fhirRelationshipType.getCode());

            //this is stored in a compound extension (extension with extensions)
            Extension outerExtension = ExtensionConverter.findOrCreateExtension(this.condition, FhirExtensionUri.PROBLEM_RELATED);
            Extension innerExtension = ExtensionConverter.createOrUpdateExtension(outerExtension, FhirExtensionUri._PROBLEM_RELATED__TYPE, stringType);

            int outerIndex = this.condition.getExtension().indexOf(outerExtension);
            int innerIndex = outerExtension.getExtension().indexOf(innerExtension);
            auditValue("extension[" + outerIndex + "].extension[" + innerIndex + "].valueString", sourceCells);
        }
    }

    public ProblemRelationshipType getParentProblemRelationship() {
        Extension outerExtension = ExtensionConverter.findExtension(this.condition, FhirExtensionUri.PROBLEM_RELATED);
        if (outerExtension == null) {
            return null;
        }
        StringType stringType = (StringType)ExtensionConverter.findExtensionValue(outerExtension, FhirExtensionUri._PROBLEM_RELATED__TYPE);
        return ProblemRelationshipType.fromCode(stringType.getValue());
    }

    public void setParentResource(Reference reference, CsvCell... sourceCells) {
        super.createOrUpdateParentResourceExtension(reference, sourceCells);
    }

    /*public void addIdentifier(Identifier identifier, CsvCell... sourceCells) {
        this.condition.addIdentifier(identifier);

        int index = this.condition.getIdentifier().size()-1;
        auditValue("identifier[" + index + "].value", sourceCells);
    }*/

    public void setVerificationStatus(Condition.ConditionVerificationStatus status, CsvCell... sourceCells) {
        this.condition.setVerificationStatus(status);

        auditValue("verificationStatus", sourceCells);
    }

    public void setSequenceNumber(int seqNo, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateIntegerExtension(getResource(),FhirExtensionUri.CONDITION_SEQUENCE_NUMBER,seqNo);
        auditIntegerExtension(extension,sourceCells);
    }

    @Override
    public Identifier addIdentifier() {
        return this.condition.addIdentifier();
    }

    @Override
    public String getIdentifierJsonPrefix(Identifier identifier) {
        int index = this.condition.getIdentifier().indexOf(identifier);
        return "identifier[" + index + "]";
    }

    @Override
    public List<Identifier> getIdentifiers() {
        return this.condition.getIdentifier();
    }

    @Override
    public void removeIdentifier(Identifier identifier) {
        this.condition.getIdentifier().remove(identifier);
    }
}
