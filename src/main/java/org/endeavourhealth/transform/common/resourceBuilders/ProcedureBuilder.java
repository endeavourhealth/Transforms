package org.endeavourhealth.transform.common.resourceBuilders;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

import java.util.Date;
import java.util.List;

public class ProcedureBuilder extends ResourceBuilderBase
                             implements HasCodeableConceptI, HasIdentifierI {

    private Procedure procedure = null;

    public ProcedureBuilder() {
        this(null);
    }

    public ProcedureBuilder(Procedure procedure) {
        this(procedure, null);
    }

    public ProcedureBuilder(Procedure procedure, ResourceFieldMappingAudit audit) {
        super(audit);

        this.procedure = procedure;
        if (this.procedure == null) {
            this.procedure = new Procedure();
            this.procedure.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_PROCEDURE));
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
        Period period = getOrCreatePerformedPeriod();

        if (dateTimeType != null) {
            period.setStartElement(dateTimeType);

            auditValue("performedPeriod.start", sourceCells);

        } else {
            period.setStartElement(null);

            //no audit required for setting to null
        }
    }

    public void setEnded(DateTimeType dateTimeType, CsvCell... sourceCells) {

        Period period = getOrCreatePerformedPeriod();

        if (dateTimeType != null) {
            period.setEndElement(dateTimeType);

            auditValue("performedPeriod.end", sourceCells);

        } else {
            period.setEndElement(null);

            //no audit required for setting to null
        }
    }

    private Period getOrCreatePerformedPeriod() {
        Period ret = null;

        //if updating an old-style procedure we may need to upgrade the DateTimeType to a Period, since Procedure supports both
        if (procedure.hasPerformed()) {
            Type performed = procedure.getPerformed();
            if (performed instanceof Period) {
                ret = (Period)performed;

            } else if (performed instanceof DateTimeType) {
                ret = new Period();
                ret.setStartElement((DateTimeType)performed); //carry over start date
                procedure.setPerformed(ret);

            } else {
                throw new RuntimeException("Unexpected type " + performed.getClass());
            }
        } else {
            ret = new Period();
            procedure.setPerformed(ret);
        }

        return ret;
    }

    /*public void setPerformed(DateTimeType dateTimeType, CsvCell... sourceCells) {
        this.procedure.setPerformed(dateTimeType);

        auditValue("performedDateTime", sourceCells);
    }

    public void setEnded(DateTimeType dateTimeType, CsvCell... sourceCells) throws Exception{
        Period period =  new Period();
        if (this.procedure.hasPerformedDateTimeType()) {
            period.setStartElement(this.procedure.getPerformedDateTimeType());
        }
        period.setEndElement(dateTimeType);
        this.procedure.setPerformed(period);
    }*/

    public void addPerformer(Reference practitionerReference, CsvCell... sourceCells) {
        Procedure.ProcedurePerformerComponent fhirPerformer = this.procedure.addPerformer();
        fhirPerformer.setActor(practitionerReference);

        int index = this.procedure.getPerformer().size()-1;
        auditValue("performer[" + index + "].actor.reference", sourceCells);
    }

    public boolean hasPerformer() {
        return this.procedure.hasPerformer();
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

    public void setSequenceNumber(int seqNo, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateIntegerExtension(getResource(),FhirExtensionUri.PROCEDURE_SEQUENCE_NUMBER,seqNo);
        auditIntegerExtension(extension,sourceCells);
    }

    public void setSpecialtyGroup(String specialtyGroup, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateStringExtension(getResource(),FhirExtensionUri.PROCEDURE_SPECIALITY_GROUP,specialtyGroup);
        auditIntegerExtension(extension,sourceCells);
    }

    public void setIsReview(boolean isReview, CsvCell... sourceCells) {
        createOrUpdateIsReviewExtension(isReview, sourceCells);
    }

    public void setIsConfidential(boolean isConfidential, CsvCell... sourceCells) {
        createOrUpdateIsConfidentialExtension(isConfidential, sourceCells);
    }

    public void setIsPrimary(boolean isPrimary, CsvCell... sourceCells) {

        //only use the extension to record the positive, remove for the negative
        if (isPrimary) {
            Extension extension = ExtensionConverter.createOrUpdateBooleanExtension(getResource(), FhirExtensionUri.IS_PRIMARY, true);
            auditBooleanExtension(extension, sourceCells);

        } else {
            ExtensionConverter.removeExtension(getResource(), FhirExtensionUri.IS_PRIMARY);
        }
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
    public CodeableConcept createNewCodeableConcept(CodeableConceptBuilder.Tag tag, boolean useExisting) {
        if (tag == CodeableConceptBuilder.Tag.Procedure_Main_Code) {
            if (this.procedure.hasCode()) {
                if (useExisting) {
                    return procedure.getCode();
                } else {
                    throw new IllegalArgumentException("Trying to add code to Procedure that already has one");
                }
            }
            this.procedure.setCode(new CodeableConcept());
            return this.procedure.getCode();

        /*} else if (tag.equals(TAG_CODEABLE_CONCEPT_CATEGORY)) {
            if (this.procedure.hasCategory()) {
                throw new IllegalArgumentException("Trying to add category to Procedure that already has one");
            }
            this.procedure.setCategory(new CodeableConcept());
            return this.procedure.getCategory();*/

        } else {
            throw new IllegalArgumentException("Invalid tag [" + tag + "]");
        }
    }

    @Override
    public String getCodeableConceptJsonPath(CodeableConceptBuilder.Tag tag, CodeableConcept codeableConcept) {
        if (tag == CodeableConceptBuilder.Tag.Procedure_Main_Code) {
            return "code";

        /*} else if (tag.equals(TAG_CODEABLE_CONCEPT_CATEGORY)) {
            return "category";*/

        } else {
            throw new IllegalArgumentException("Invalid tag [" + tag + "]");
        }
    }

    @Override
    public void removeCodeableConcept(CodeableConceptBuilder.Tag tag, CodeableConcept codeableConcept) {

        if (tag == CodeableConceptBuilder.Tag.Procedure_Main_Code) {
            this.procedure.setCode(null);

        /*} else if (tag.equals(TAG_CODEABLE_CONCEPT_CATEGORY)) {
            this.procedure.setCategory(null);*/

        } else {
            throw new IllegalArgumentException("Invalid tag [" + tag + "]");
        }
    }

    public void setCategory(String category, CsvCell... sourceCells) {
        if (Strings.isNullOrEmpty(category)) {
            this.procedure.setCategory(null);

        } else {
            CodeableConcept codeableConcept = new CodeableConcept();
            Coding coding = codeableConcept.addCoding();
            coding.setSystem(FhirValueSetUri.VALUE_SET_CONDITION_CATEGORY);
            coding.setCode(category);
            this.procedure.setCategory(codeableConcept);

            auditValue("category.coding[0].code", sourceCells);
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

    public void setContext(String context, CsvCell... sourceCells) {
        super.createOrUpdateContextExtension(context, sourceCells);
    }
}
