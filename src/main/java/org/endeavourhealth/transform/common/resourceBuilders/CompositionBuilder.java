package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

import java.util.Date;
import java.util.List;

public class CompositionBuilder extends ResourceBuilderBase implements HasCodeableConceptI {

    private Composition composition = null;

    public CompositionBuilder() {
        this(null);
    }

    public CompositionBuilder(Composition composition) {
        this(composition, null);
    }

    public CompositionBuilder(Composition composition, ResourceFieldMappingAudit audit) {
        super(audit);

        this.composition = composition;
        if (this.composition == null) {
            this.composition = new Composition();
            this.composition.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_COMPOSITION));
        }
    }

    public void setIdentifier(Identifier identifier, CsvCell... sourceCells) {
        this.composition.setIdentifier(identifier);

        auditValue("identifier", sourceCells);
    }

    public void setDate(Date date, CsvCell... sourceCells) {
        this.composition.setDate(date);

        auditValue("date", sourceCells);
    }

    public void setTitle(String title, CsvCell... sourceCells) {
        this.composition.setTitle(title);

        auditValue("title", sourceCells);
    }

    public void setStatus(Composition.CompositionStatus status, CsvCell... sourceCells) {
        this.composition.setStatus(status);

        auditValue("status", sourceCells);
    }

    public void setPatientSubject(Reference patientReference, CsvCell... sourceCells) {
        this.composition.setSubject(patientReference);

        auditValue("subject", sourceCells);
    }

    public void setAuthor(Reference userReference, CsvCell... sourceCells) {
        this.composition.addAuthor(userReference);

        auditValue("author", sourceCells);
    }

    public void addSection(String title, String id, String jsonData) throws Exception {

        Composition.SectionComponent section = this.composition.addSection();
        section.setId(id);
        section.setTitle(title);

        Narrative data = new Narrative();
        data.setStatus(Narrative.NarrativeStatus.ADDITIONAL);
        data.setDivAsString(jsonData);
        section.setText(data);

        //section.setUserData(title, jsonData);
    }

    public Composition.SectionComponent getSection(String title) {

        List<Composition.SectionComponent> sections = this.composition.getSection();
        for (Composition.SectionComponent section : sections) {
            if (section.getTitle().equals(title)) {
                return section;
            }
        }
        return null;
    }

    public void setIsConfidential(boolean isConfidential, CsvCell... sourceCells) {
        createOrUpdateIsConfidentialExtension(isConfidential, sourceCells);
    }

    @Override
    public CodeableConcept createNewCodeableConcept(CodeableConceptBuilder.Tag tag, boolean useExisting) {
        if (tag == CodeableConceptBuilder.Tag.Composition_Type) {
            if (this.composition.hasType()) {
                if (useExisting) {
                    return composition.getType();
                } else {
                    throw new IllegalArgumentException("Trying to add new type code to Composition that already has one");
                }
            }
            this.composition.setType(new CodeableConcept());
            return this.composition.getType();

        } else {
            throw new IllegalArgumentException("Invalid tag [" + tag + "]");
        }
    }

    @Override
    public String getCodeableConceptJsonPath(CodeableConceptBuilder.Tag tag, CodeableConcept codeableConcept) {
        if (tag == CodeableConceptBuilder.Tag.Composition_Type) {
            return "type";

        } else {
            throw new IllegalArgumentException("Invalid tag [" + tag + "]");
        }
    }

    @Override
    public void removeCodeableConcept(CodeableConceptBuilder.Tag tag, CodeableConcept codeableConcept) {

        if (tag == CodeableConceptBuilder.Tag.Composition_Type) {
            this.composition.setType(null);

        } else {
            throw new IllegalArgumentException("Invalid tag [" + tag + "]");
        }
    }

    @Override
    public DomainResource getResource() {
        return composition;
    }

}
