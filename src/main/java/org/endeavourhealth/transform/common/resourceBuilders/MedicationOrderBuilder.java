package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.endeavourhealth.common.fhir.QuantityHelper;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

import java.math.BigDecimal;
import java.util.Date;

public class MedicationOrderBuilder extends ResourceBuilderBase
                                    implements HasCodeableConceptI {

    private MedicationOrder medicationOrder = null;

    public MedicationOrderBuilder() {
        this(null);
    }

    public MedicationOrderBuilder(MedicationOrder medicationOrder) {
        this(medicationOrder, null);
    }

    public MedicationOrderBuilder(MedicationOrder medicationOrder, ResourceFieldMappingAudit audit) {
        super(audit);

        this.medicationOrder = medicationOrder;
        if (this.medicationOrder == null) {
            this.medicationOrder = new MedicationOrder();
            this.medicationOrder.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_MEDICATION_ORDER));
        }
    }

    @Override
    public DomainResource getResource() {
        return medicationOrder;
    }

    public void setPatient(Reference patientReference, CsvCell... sourceCells) {
        this.medicationOrder.setPatient(patientReference);

        auditValue("patient.reference", sourceCells);
    }

    public void setDateWritten(DateTimeType dateTime, CsvCell... sourceCells) {
        this.medicationOrder.setDateWrittenElement(dateTime);

        auditValue("dateWritten", sourceCells);
    }

    public void setIsConfidential(boolean isConfidential, CsvCell... sourceCells) {
        super.createOrUpdateIsConfidentialExtension(isConfidential, sourceCells);
    }

    public void setRecordedDate(Date date, CsvCell... sourceCells) {
        super.createOrUpdateRecordedDateExtension(date, sourceCells);
    }

    public void setRecordedBy(Reference reference, CsvCell... sourceCells) {
        super.createOrUpdateRecordedByExtension(reference, sourceCells);
    }

    public void setEncounter(Reference reference, CsvCell... sourceCells) {
        super.createOrUpdateEncounterExtension(reference, sourceCells);
    }

    public void setMedicationStatementReference(Reference medicationStatementReference, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateExtension(this.medicationOrder, FhirExtensionUri.MEDICATION_ORDER_AUTHORISATION, medicationStatementReference);

        auditReferenceExtension(extension, sourceCells);
    }

    public void setReason(Reference conditionReference, CsvCell... sourceCells) {
        this.medicationOrder.setReason(conditionReference);

        auditValue("reasonReference.reference", sourceCells);
    }

    public void setPrescriber(Reference practitionerReference, CsvCell... sourceCells) {
        this.medicationOrder.setPrescriber(practitionerReference);

        auditValue("prescriber.reference", sourceCells);
    }

    public void setDose(String dose, CsvCell... sourceCells) {
        MedicationOrder.MedicationOrderDosageInstructionComponent fhirDose = this.medicationOrder.addDosageInstruction();
        fhirDose.setText(dose);

        int index = this.medicationOrder.getDosageInstruction().size() - 1;
        auditValue("dosageInstruction[" + index + "].text", sourceCells);
    }

    public void setNhsCost(Double value, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateDecimalExtension(this.medicationOrder, FhirExtensionUri.MEDICATION_ORDER_ESTIMATED_COST, value);

        auditDecimalExtension(extension, sourceCells);
    }

    private MedicationOrder.MedicationOrderDispenseRequestComponent getDispenseRequest() {
        MedicationOrder.MedicationOrderDispenseRequestComponent ret = null;
        if (this.medicationOrder.hasDispenseRequest()) {
            ret = this.medicationOrder.getDispenseRequest();
        } else {
            ret = new MedicationOrder.MedicationOrderDispenseRequestComponent();
            this.medicationOrder.setDispenseRequest(ret);
        }
        return ret;
    }

    private SimpleQuantity getDispenseRequestQuantity() {
        SimpleQuantity quantity = null;
        MedicationOrder.MedicationOrderDispenseRequestComponent dispenseRequest = getDispenseRequest();
        if (dispenseRequest.hasQuantity()) {
            quantity = dispenseRequest.getQuantity();
        } else {
            quantity = new SimpleQuantity();
            dispenseRequest.setQuantity(quantity);
        }
        return quantity;
    }

    public void setQuantityValue(Double value, CsvCell... sourceCells) {
        SimpleQuantity quantity = getDispenseRequestQuantity();
        quantity.setValue(BigDecimal.valueOf(value.doubleValue()));

        auditValue("dispenseRequest.quantity.value", sourceCells);
    }

    public void setQuantityUnit(String unit, CsvCell... sourceCells) {
        SimpleQuantity quantity = getDispenseRequestQuantity();
        quantity.setUnit(unit);

        auditValue("dispenseRequest.quantity.unit", sourceCells);
    }

    public void setDurationDays(Integer days, CsvCell... sourceCells) {
        Duration duration = QuantityHelper.createDuration(days, "days");
        MedicationOrder.MedicationOrderDispenseRequestComponent dispenseRequest = getDispenseRequest();
        dispenseRequest.setExpectedSupplyDuration(duration);

        auditValue("dispenseRequest.expectedSupplyDuration.value", sourceCells);
    }


    @Override
    public CodeableConcept createNewCodeableConcept(CodeableConceptBuilder.Tag tag, boolean useExisting) {

        if (tag == CodeableConceptBuilder.Tag.Medication_Order_Drug_Code) {

            try {
                if (this.medicationOrder.hasMedicationCodeableConcept()) {
                    if (useExisting) {
                        return medicationOrder.getMedicationCodeableConcept();
                    } else {
                        throw new IllegalArgumentException("Trying to add new medication to MedicationOrder when it already has one");
                    }
                }

                CodeableConcept codeableConcept = new CodeableConcept();
                this.medicationOrder.setMedication(codeableConcept);
                return codeableConcept;

            } catch (Exception ex) {
                //we should never get this exception raised, but if we do, just wrap in a runtime exception and throw up
                throw new RuntimeException(ex);
            }

        } else {
            throw new IllegalArgumentException("Unknown tag [" + tag + "]");
        }
    }

    @Override
    public String getCodeableConceptJsonPath(CodeableConceptBuilder.Tag tag, CodeableConcept codeableConcept) {
        if (tag == CodeableConceptBuilder.Tag.Medication_Order_Drug_Code) {
            return "medicationCodeableConcept";

        } else {
            throw new IllegalArgumentException("Unknown tag [" + tag + "]");
        }
    }

    @Override
    public void removeCodeableConcept(CodeableConceptBuilder.Tag tag, CodeableConcept codeableConcept) {
        if (tag == CodeableConceptBuilder.Tag.Medication_Order_Drug_Code) {
            this.medicationOrder.setMedication(null);

        } else {
            throw new IllegalArgumentException("Unknown tag [" + tag + "]");
        }
    }
}
