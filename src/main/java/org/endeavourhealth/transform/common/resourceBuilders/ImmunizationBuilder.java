package org.endeavourhealth.transform.common.resourceBuilders;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.AnnotationHelper;
import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

import java.util.Date;

public class ImmunizationBuilder extends ResourceBuilderBase
                                 implements HasCodeableConceptI {

    public static final String TAG_VACCINE_CODEABLE_CONCEPT = "VaccineCode";
    public static final String TAG_SITE_CODEABLE_CONCEPT = "SiteCode";
    public static final String TAG_ROUTE_CODEABLE_CONCEPT = "RouteCode";

    private Immunization immunization = null;

    public ImmunizationBuilder() {
        this(null);
    }

    public ImmunizationBuilder(Immunization immunization) {
        this.immunization = immunization;
        if (this.immunization == null) {
            this.immunization = new Immunization();
            this.immunization.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_IMMUNIZATION));
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

    public void setLotNumber(String lotNumber, CsvCell... sourceCells) {
        this.immunization.setLotNumber(lotNumber);

        auditValue("lotNumber", sourceCells);
    }

    public void setReason(String reason, CsvCell... sourceCells) {
        Immunization.ImmunizationExplanationComponent immsExplanationComponent = new Immunization.ImmunizationExplanationComponent();
        if (!Strings.isNullOrEmpty(reason)) {
            immsExplanationComponent.addReason().setText(reason);
        } else {
            immsExplanationComponent.addReasonNotGiven();
        }
        this.immunization.setExplanation(immsExplanationComponent);

        auditValue("explanation.reason", sourceCells);
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

        if (tag.equals(TAG_VACCINE_CODEABLE_CONCEPT)) {
            if (this.immunization.hasVaccineCode()) {
                throw new IllegalArgumentException("Trying to add new Vaccine code to Immunization when it already has one");
            }

            CodeableConcept codeableConcept = new CodeableConcept();
            this.immunization.setVaccineCode(codeableConcept);
            return codeableConcept;

        } else if (tag.equals(TAG_SITE_CODEABLE_CONCEPT)) {
            if (this.immunization.hasSite()) {
                throw new IllegalArgumentException("Trying to add a new Site code to Immunization when it already has one");
            }

            CodeableConcept codeableConcept = new CodeableConcept();
            this.immunization.setSite(codeableConcept);
            return codeableConcept;

        } else if (tag.equals(TAG_ROUTE_CODEABLE_CONCEPT)) {
            if (this.immunization.hasRoute()) {
                throw new IllegalArgumentException("Trying to add a new Route code to Immunization when it already has one");
            }

            CodeableConcept codeableConcept = new CodeableConcept();
            this.immunization.setRoute(codeableConcept);
            return codeableConcept;
        } else {
            throw new IllegalArgumentException("Unknown tag " + tag);
        }
    }

    @Override
    public String getCodeableConceptJsonPath(String tag, CodeableConcept codeableConcept) {

        if (tag.equals(TAG_VACCINE_CODEABLE_CONCEPT)) {
            return "vaccineCode";

        } else if (tag.equals(TAG_SITE_CODEABLE_CONCEPT)) {
            return "site";

        } else if (tag.equals(TAG_ROUTE_CODEABLE_CONCEPT)) {
            return "route";

        } else {
            throw new IllegalArgumentException("Unknown tag " + tag);
        }
    }

    @Override
    public void removeCodeableConcepts(String tag) {

        if (tag.equals(TAG_VACCINE_CODEABLE_CONCEPT)) {
            this.immunization.setVaccineCode(null);

        } else if (tag.equals(TAG_SITE_CODEABLE_CONCEPT)) {
            this.immunization.setSite(null);

        } else if (tag.equals(TAG_ROUTE_CODEABLE_CONCEPT)) {
            this.immunization.setRoute(null);

        } else {
            throw new IllegalArgumentException("Unknown tag " + tag);
        }
    }
}
