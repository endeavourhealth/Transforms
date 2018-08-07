package org.endeavourhealth.transform.common.resourceBuilders;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.AnnotationHelper;
import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
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
        this(immunization, null);
    }

    public ImmunizationBuilder(Immunization immunization, ResourceFieldMappingAudit audit) {
        super(audit);

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

    public void setLocation(Reference locationReference, CsvCell... sourceCells) {
        this.immunization.setLocation(locationReference);

        auditValue("location.reference", sourceCells);
    }

    public void setSite(String site, CsvCell... sourceCells) {
        if (!Strings.isNullOrEmpty(site)) {
            CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(site);
            this.immunization.setSite(codeableConcept);

            auditValue("site", sourceCells);
        }
    }

    public void setRoute(String route, CsvCell... sourceCells) {
        if (!Strings.isNullOrEmpty(route)) {
            CodeableConcept codeableConcept = CodeableConceptHelper.createCodeableConcept(route);
            this.immunization.setRoute(codeableConcept);

            auditValue("route", sourceCells);
        }
    }

    public void setVaccineCode(CodeableConcept code, CsvCell... sourceCells) {
        if (code != null) {
            this.immunization.setVaccineCode(code);

            auditValue("vaccineCode", sourceCells);
        }
    }

    public void setVaccinationProtocol(Immunization.ImmunizationVaccinationProtocolComponent protocol, CsvCell... sourceCells) {
        if (protocol != null) {
            this.immunization.addVaccinationProtocol(protocol);

            auditValue("vaccinationProtocol", sourceCells);
        }
    }

    public void setDoseQuantity(SimpleQuantity dose, CsvCell... sourceCells) {
        this.immunization.setDoseQuantity(dose);

        auditValue("doseQuantity", sourceCells);
    }

    public void setExpirationDate(Date expirationDate, CsvCell... sourceCells) {
        this.immunization.setExpirationDate(expirationDate);

        auditValue("expirationDate", sourceCells);
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
    public CodeableConcept createNewCodeableConcept(CodeableConceptBuilder.Tag tag) {

        if (tag == CodeableConceptBuilder.Tag.Immunization_Main_Code) {
            if (this.immunization.hasVaccineCode()) {
                throw new IllegalArgumentException("Trying to add new Vaccine code to Immunization when it already has one");
            }

            CodeableConcept codeableConcept = new CodeableConcept();
            this.immunization.setVaccineCode(codeableConcept);
            return codeableConcept;

        } else if (tag == CodeableConceptBuilder.Tag.Immunization_Site) {
            if (this.immunization.hasSite()) {
                throw new IllegalArgumentException("Trying to add a new Site code to Immunization when it already has one");
            }

            CodeableConcept codeableConcept = new CodeableConcept();
            this.immunization.setSite(codeableConcept);
            return codeableConcept;

        } else if (tag == CodeableConceptBuilder.Tag.Immunization_Route) {
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
    public String getCodeableConceptJsonPath(CodeableConceptBuilder.Tag tag, CodeableConcept codeableConcept) {

        if (tag == CodeableConceptBuilder.Tag.Immunization_Main_Code) {
            return "vaccineCode";

        } else if (tag == CodeableConceptBuilder.Tag.Immunization_Site) {
            return "site";

        } else if (tag == CodeableConceptBuilder.Tag.Immunization_Route) {
            return "route";

        } else {
            throw new IllegalArgumentException("Unknown tag " + tag);
        }
    }

    @Override
    public void removeCodeableConcept(CodeableConceptBuilder.Tag tag, CodeableConcept codeableConcept) {

        if (tag == CodeableConceptBuilder.Tag.Immunization_Main_Code) {
            this.immunization.setVaccineCode(null);

        } else if (tag == CodeableConceptBuilder.Tag.Immunization_Site) {
            this.immunization.setSite(null);

        } else if (tag == CodeableConceptBuilder.Tag.Immunization_Route) {
            this.immunization.setRoute(null);

        } else {
            throw new IllegalArgumentException("Unknown tag " + tag);
        }
    }

    private Immunization.ImmunizationVaccinationProtocolComponent getProtocolComponent() {
        if (this.immunization.hasVaccinationProtocol()) {
            return this.immunization.getVaccinationProtocol().get(0);
        } else {
            Immunization.ImmunizationVaccinationProtocolComponent protocolComponent = new Immunization.ImmunizationVaccinationProtocolComponent();
            this.immunization.addVaccinationProtocol(protocolComponent);
            return protocolComponent;
        }
    }

    public void setProtocolSequenceNumber(int val, CsvCell... sourceCells) {
        getProtocolComponent().setDoseSequence(val);

        auditValue("vaccinationProtocol[0].doseSequence", sourceCells);
    }

    public void setProtocolDescription(String desc, CsvCell... sourceCells) {
        getProtocolComponent().setDescription(desc);

        auditValue("vaccinationProtocol[0].description", sourceCells);
    }

    public void setProtocolSeriesName(String name, CsvCell... sourceCells) {
        getProtocolComponent().setSeries(name);

        auditValue("vaccinationProtocol[0].series", sourceCells);
    }
}
