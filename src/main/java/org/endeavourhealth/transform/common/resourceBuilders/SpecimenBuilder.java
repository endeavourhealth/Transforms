package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

import java.util.Date;

public class SpecimenBuilder extends ResourceBuilderBase
                            implements HasCodeableConceptI {

    private Specimen specimen = null;

    public SpecimenBuilder() {
        this(null);
    }

    public SpecimenBuilder(Specimen specimen) {
        this.specimen = specimen;
        if (this.specimen == null) {
            this.specimen = new Specimen();
            this.specimen.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_SPECIMIN));
        }
    }

    @Override
    public DomainResource getResource() {
        return specimen;
    }

    public void setPatient(Reference patientReference, CsvCell... sourceCells) {
        this.specimen.setSubject(patientReference);

        auditValue("subject.reference", sourceCells);
    }

    private Specimen.SpecimenCollectionComponent getCollectionElement() {
        if (this.specimen.hasCollection()) {
            return this.specimen.getCollection();
        } else {
            Specimen.SpecimenCollectionComponent fhirCollection = new Specimen.SpecimenCollectionComponent();
            this.specimen.setCollection(fhirCollection);
            return fhirCollection;
        }
    }

    public void setCollectedBy(Reference practitionerReference, CsvCell... sourceCells) {
        getCollectionElement().setCollector(practitionerReference);

        auditValue("collection.collector.reference", sourceCells);
    }

    public void setNotes(String associatedText, CsvCell... sourceCells) {
        getCollectionElement().addComment(associatedText);

        auditValue("collection.comment");
    }

    public void setCollectedDate(DateTimeType dateTimeType, CsvCell... sourceCells) {
        getCollectionElement().setCollected(dateTimeType);

        auditValue("collection.collected.valueDateTime", sourceCells);
    }

    public void setRecordedDate(Date recordedDate, CsvCell... sourceCells) {
        createOrUpdateRecordedDateExtension(recordedDate, sourceCells);
    }

    public void setRecordedBy(Reference practitionerReference, CsvCell... sourceCells) {
        createOrUpdateRecordedByExtension(practitionerReference, sourceCells);
    }

    public void setEncounter(Reference encounterReference, CsvCell... sourceCells) {
        createOrUpdateEncounterExtension(encounterReference, sourceCells);
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

    @Override
    public CodeableConcept getOrCreateCodeableConcept(String tag) {
        if (!this.specimen.hasType()) {
            this.specimen.setType(new CodeableConcept());
        }
        return this.specimen.getType();
    }

    @Override
    public String getCodeableConceptJsonPath(String tag) {
        return "type";
    }

    public void setParentResource(Reference reference, CsvCell... sourceCells) {
        super.createOrUpdateParentResourceExtension(reference, sourceCells);
    }
}
