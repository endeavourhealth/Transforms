package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.common.fhir.QuantityHelper;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

public class ObservationBuilder extends ResourceBuilderBase
                                implements HasCodeableConceptI, HasIdentifierI {

    public static final String TAG_MAIN_CODEABLE_CONCEPT = "MainCode";
    public static final String TAG_COMPONENT_CODEABLE_CONCEPT = "ComponentCode";
    public static final String TAG_RANGE_MEANING_CODEABLE_CONCEPT = "RangeMeaning";

    private Observation observation = null;

    public ObservationBuilder() {
        this(null);
    }

    public ObservationBuilder(Observation observation) {
        this.observation = observation;
        if (this.observation == null) {
            this.observation = new Observation();
            this.observation.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_OBSERVATION));
        }
    }

    @Override
    public DomainResource getResource() {
        return observation;
    }

    public void setPatient(Reference patientReference, CsvCell... sourceCells) {
        this.observation.setSubject(patientReference);

        auditValue("subject.reference", sourceCells);
    }

    public void setStatus(Observation.ObservationStatus status, CsvCell... sourceCells) {
        this.observation.setStatus(status);

        auditValue("status", sourceCells);
    }

    public void setEffectiveDate(DateTimeType dateTimeType, CsvCell... sourceCells) {
        this.observation.setEffective(dateTimeType);

        auditValue("effectiveDateTime", sourceCells);
    }

    public void setClinician(Reference practitionerReference, CsvCell... sourceCells) {
        //we only support having one performer, so ensure we don't accidentally end up with multiple if amending an observation
        if (this.observation.hasPerformer()) {
            this.observation.getPerformer().clear();
        }
        this.observation.addPerformer(practitionerReference);

        auditValue("performer[0].reference", sourceCells);
    }

    public void setNotes(String notes, CsvCell... sourceCells) {
        this.observation.setComments(notes);

        auditValue("comments", sourceCells);
    }

    private Quantity findOrCreateQuantity() {
        try {
            if (this.observation.hasValueQuantity()) {
                return this.observation.getValueQuantity();
            } else {
                Quantity quantity = new Quantity();
                this.observation.setValue(quantity);
                return quantity;
            }
        } catch (Exception ex) {
            //the above code should never throw an exception, but the functions are delcared to potentially do so,
            //in which case just wrap in runtime exception and throw again
            throw new RuntimeException(ex);
        }
    }

    public void setValue(Double value, CsvCell... sourceCells) {
        findOrCreateQuantity().setValue(BigDecimal.valueOf(value));

        auditValue("valueQuantity.value", sourceCells);
    }

    public void setUnits(String units, CsvCell... sourceCells) {
        findOrCreateQuantity().setUnit(units);

        auditValue("valueQuantity.unit", sourceCells);
    }

    public void setValueComparator(Quantity.QuantityComparator comparatorValue, CsvCell... sourceCells) {
        findOrCreateQuantity().setComparator(comparatorValue);

        auditValue("valueQuantity.comparator", sourceCells);
    }

    public void setEncounter(Reference encounterReference, CsvCell... sourceCells) {
        this.observation.setEncounter(encounterReference);

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

    private Observation.ObservationReferenceRangeComponent findOrCreateReferenceRangeElement() {
        if (this.observation.hasReferenceRange()) {
            return this.observation.getReferenceRange().get(0);
        } else {
            return this.observation.addReferenceRange();
        }
    }


    public void setRecommendedRangeLow(Double value, String units, Quantity.QuantityComparator comparator, CsvCell... sourceCells) {
        SimpleQuantity quantity = QuantityHelper.createSimpleQuantity(value, units, comparator);
        findOrCreateReferenceRangeElement().setLow(quantity);

        auditValue("referenceRange[0].low", sourceCells);
    }

    public void setRecommendedRangeHigh(Double value, String units, Quantity.QuantityComparator comparator, CsvCell... sourceCells) {
        SimpleQuantity quantity = QuantityHelper.createSimpleQuantity(value, units, comparator);
        findOrCreateReferenceRangeElement().setHigh(quantity);

        auditValue("referenceRange[0].high", sourceCells);
    }

    private boolean hasChildObservation(Reference reference) {
        if (this.observation.hasRelated()) {
            for (Observation.ObservationRelatedComponent related: this.observation.getRelated()) {

                Reference relatedReference = related.getTarget();

                if (related.getType() == Observation.ObservationRelationshipType.HASMEMBER
                    && ReferenceHelper.equals(reference, relatedReference)) {
                    return true;
                }
            }
        }

        return false;
    }

    public boolean addChildObservation(Reference reference, CsvCell... sourceCells) {

        //if this child observation is already present, this does nothing
        if (hasChildObservation(reference)) {
            return false;
        }

        Observation.ObservationRelatedComponent fhirRelation = this.observation.addRelated();
        fhirRelation.setType(Observation.ObservationRelationshipType.HASMEMBER);
        fhirRelation.setTarget(reference);

        int index = this.observation.getRelated().indexOf(fhirRelation);
        auditValue("related[" + index + "].target.reference", sourceCells);
        return true;
    }

    private Observation.ObservationComponentComponent getLastComponent() {
        int index = getLastComponentIndex();
        return this.observation.getComponent().get(index);
    }

    private int getLastComponentIndex() {
        //hasComponent actually checks to see if the component is empty, so we can't use it when we know it's empty
        //if (this.observation.hasComponent()) {
        List<Observation.ObservationComponentComponent> components = this.observation.getComponent();
        if (components != null) {
            return this.observation.getComponent().size()-1;
        } else {
            throw new IllegalArgumentException("No components in observation");
        }
    }

    private Quantity getOrCreateLastComponentQuantity() {
        Observation.ObservationComponentComponent component = getLastComponent();
        if (!component.hasValue()) {
            component.setValue(new Quantity());
        }
        return (Quantity)component.getValue();
    }

    public void addComponent() {
        this.observation.addComponent();
    }



    public void setComponentValue(Double value, CsvCell... sourceCells) {
        getOrCreateLastComponentQuantity().setValue(BigDecimal.valueOf(value));

        auditValue("component[" + getLastComponentIndex() + "].valueQuantity.value", sourceCells);
    }

    public void setComponentUnit(String units, CsvCell... sourceCells) {
        getOrCreateLastComponentQuantity().setUnit(units);

        auditValue("component[" + getLastComponentIndex() + "].valueQuantity.unit", sourceCells);
    }

    public void addIdentifier(Identifier identifier, CsvCell... sourceCells) {
        this.observation.addIdentifier(identifier);

        int index = this.observation.getIdentifier().size()-1;
        auditValue("identifier[" + index + "].value", sourceCells);
    }

    public void setParentResource(Reference reference, CsvCell... sourceCells) {
        super.createOrUpdateParentResourceExtension(reference, sourceCells);
    }

    @Override
    public CodeableConcept createNewCodeableConcept(String tag) {
        //depending on the tag, we may be setting the main codeable concept or the one in the last component added
        if (tag.equals(TAG_MAIN_CODEABLE_CONCEPT)) {
            if (this.observation.hasCode()) {
                throw new IllegalArgumentException("Trying to add code to Observation when it already has one");
            }

            this.observation.setCode(new CodeableConcept());
            return this.observation.getCode();

        } else if (tag.equals(TAG_COMPONENT_CODEABLE_CONCEPT)) {
            Observation.ObservationComponentComponent component = getLastComponent();
            if (component.hasCode()) {
                throw new IllegalArgumentException("Trying to add code to Observation Component when it already has one");
            }
            component.setCode(new CodeableConcept());
            return component.getCode();

        } else if (tag.equals(TAG_RANGE_MEANING_CODEABLE_CONCEPT)) {
            Observation.ObservationReferenceRangeComponent rangeComponent = findOrCreateReferenceRangeElement();
            if (rangeComponent.hasMeaning()) {
                throw new IllegalArgumentException("Trying to set meaning on Observation when already set");
            }
            CodeableConcept codeableConcept = new CodeableConcept();
            rangeComponent.setMeaning(codeableConcept);
            return codeableConcept;

        } else {
            throw new IllegalArgumentException("Unknown tag " + tag);
        }
    }

    @Override
    public String getCodeableConceptJsonPath(String tag, CodeableConcept codeableConcept) {
        if (tag.equals(TAG_MAIN_CODEABLE_CONCEPT)) {
            return "code";

        } else if (tag.equals(TAG_COMPONENT_CODEABLE_CONCEPT)) {
            return "component[" + getLastComponentIndex() + "].code";

        } else if (tag.equals(TAG_RANGE_MEANING_CODEABLE_CONCEPT)) {
            return "referenceRange[0].meaning";

        } else {
            throw new IllegalArgumentException("Unknown tag " + tag);
        }
    }

    @Override
    public Identifier addIdentifier() {
        return this.observation.addIdentifier();
    }

    @Override
    public String getIdentifierJsonPrefix(Identifier identifier) {
        int index = this.observation.getIdentifier().indexOf(identifier);
        return "identifier[" + index + "]";
    }


}
