package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.common.fhir.CodingHelper;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.common.fhir.schema.MedicationAuthorisationType;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

import java.math.BigDecimal;
import java.util.Date;

public class MedicationStatementBuilder extends ResourceBuilderBase
                                        implements HasCodeableConceptI {

    private MedicationStatement medicationStatement = null;

    public MedicationStatementBuilder() {
        this(null);
    }

    public MedicationStatementBuilder(MedicationStatement medicationStatement) {
        this.medicationStatement = medicationStatement;
        if (this.medicationStatement == null) {
            this.medicationStatement = new MedicationStatement();
            this.medicationStatement.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_MEDICATION_AUTHORISATION));
        }
    }

    @Override
    public DomainResource getResource() {
        return medicationStatement;
    }

    public void setPatient(Reference patientReference, CsvCell... sourceCells) {
        this.medicationStatement.setPatient(patientReference);

        auditValue("patient.reference", sourceCells);
    }

    public void setInformationSource(Reference practitionerReference, CsvCell... sourceCells) {
        this.medicationStatement.setInformationSource(practitionerReference);

        auditValue("informationSource.reference", sourceCells);
    }

    public void setAssertedDate(DateTimeType date, CsvCell... sourceCells) {
        this.medicationStatement.setDateAssertedElement(date);

        auditValue("dateAsserted", sourceCells);
    }

    public void setStatus(MedicationStatement.MedicationStatementStatus status, CsvCell... sourceCells) {
        this.medicationStatement.setStatus(status);

        auditValue("status", sourceCells);
    }

    public void setDose(String dose, CsvCell... sourceCells) {
        MedicationStatement.MedicationStatementDosageComponent fhirDose = this.medicationStatement.addDosage();
        fhirDose.setText(dose);

        int index = this.medicationStatement.getDosage().size()-1;
        auditValue("dosage[" + index + "].text", sourceCells);
    }

    public void setNumberIssuesAuthorised(int num, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdatePositiveIntExtension(this.medicationStatement, FhirExtensionUri.MEDICATION_AUTHORISATION_NUMBER_OF_REPEATS_ALLOWED, num);

        auditIntegerExtension(extension, sourceCells);
    }

    public void setNumberIssuesIssued(int num, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdatePositiveIntExtension(this.medicationStatement, FhirExtensionUri.MEDICATION_AUTHORISATION_NUMBER_OF_REPEATS_ISSUED, num);

        auditIntegerExtension(extension, sourceCells);
    }

    public void setReasonForUse(Reference conditionReference, CsvCell... sourceCells) {
        this.medicationStatement.setReasonForUse(conditionReference);

        auditValue("reasonForUseReference.reference", sourceCells);
    }

    public void setCancellationDate(Date date, CsvCell... sourceCells) {
        //the cancellation extension is a compound extension, so we have one extension inside another
        Extension outerExtension = ExtensionConverter.findOrCreateExtension(this.medicationStatement, FhirExtensionUri.MEDICATION_AUTHORISATION_CANCELLATION);
        Extension innerExtension = ExtensionConverter.findOrCreateExtension(outerExtension, "date");
        innerExtension.setValue(new DateType(date));

        int outerIndex = this.medicationStatement.getExtension().indexOf(outerExtension);
        int innerIndex = outerExtension.getExtension().indexOf(innerExtension);
        auditValue("extension[" + outerIndex + "].extension[" + innerIndex + "].valueDate", sourceCells);

    }

    public void setRecordedBy(Reference reference, CsvCell... sourceCells) {
        super.createOrUpdateRecordedByExtension(reference, sourceCells);
    }

    public void setRecordedDate(Date date, CsvCell... sourceCells) {
        super.createOrUpdateRecordedDateExtension(date, sourceCells);
    }

    public void setAuthorisationType(MedicationAuthorisationType fhirAuthorisationType, CsvCell... sourceCells) {
        Coding fhirCoding = CodingHelper.createCoding(fhirAuthorisationType);
        Extension extension = ExtensionConverter.createOrUpdateExtension(this.medicationStatement, FhirExtensionUri.MEDICATION_AUTHORISATION_TYPE, fhirCoding);

        auditCodingExtension(extension, sourceCells);
    }

    public void setIsConfidential(boolean isConfidential, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateBooleanExtension(this.medicationStatement, FhirExtensionUri.IS_CONFIDENTIAL, isConfidential);

        auditBooleanExtension(extension, sourceCells);
    }

    private Quantity findOrAddQuantity(Extension extension) {
        Quantity quantity = (Quantity)extension.getValue();
        if (quantity == null) {
            quantity = new Quantity();
            extension.setValue(quantity);
        }
        return quantity;
    }

    public void setQuantityValue(Double value, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.findOrCreateExtension(this.medicationStatement, FhirExtensionUri.MEDICATION_AUTHORISATION_QUANTITY);
        Quantity quantity = findOrAddQuantity(extension);
        quantity.setValue(BigDecimal.valueOf(value));

        auditQuantityValueExtension(extension, sourceCells);
    }

    public void setQuantityUnit(String unit, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.findOrCreateExtension(this.medicationStatement, FhirExtensionUri.MEDICATION_AUTHORISATION_QUANTITY);
        Quantity quantity = findOrAddQuantity(extension);
        quantity.setUnit(unit);

        auditQuantityUnitExtension(extension, sourceCells);
    }

    public void setFirstIssueDate(DateType issueDateType, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateExtension(this.medicationStatement, FhirExtensionUri.MEDICATION_AUTHORISATION_FIRST_ISSUE_DATE, issueDateType);

        auditDateExtension(extension, sourceCells);
    }

    public void setLastIssueDate(DateType issueDateType, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateExtension(this.medicationStatement, FhirExtensionUri.MEDICATION_AUTHORISATION_MOST_RECENT_ISSUE_DATE, issueDateType);

        auditDateExtension(extension, sourceCells);
    }

    @Override
    public CodeableConcept createNewCodeableConcept(String tag) {
        if (this.medicationStatement.hasMedication()) {
            throw new IllegalArgumentException("Trying to add new code to MedicationStatement when it already has one");
        }
        this.medicationStatement.setMedication(new CodeableConcept());
        return (CodeableConcept)this.medicationStatement.getMedication();
    }

    @Override
    public String getCodeableConceptJsonPath(String tag, CodeableConcept codeableConcept) {
        return "medicationCodeableConcept";
    }

    @Override
    public void removeCodeableConcepts(String tag) {
        this.medicationStatement.setMedication(null);
    }
}
