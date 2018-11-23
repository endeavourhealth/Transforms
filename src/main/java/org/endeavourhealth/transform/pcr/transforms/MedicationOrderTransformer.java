package org.endeavourhealth.transform.pcr.transforms;

import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.schema.MedicationAuthorisationType;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.pcr.FhirToPcrCsvTransformer;
import org.endeavourhealth.transform.pcr.PcrTransformParams;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.endeavourhealth.transform.pcr.outputModels.MedicationAmount;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Date;

public class MedicationOrderTransformer extends AbstractTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(MedicationOrderTransformer.class);

    public boolean shouldAlwaysTransform() {
        return true;
    }

    protected void transformResource(Long pcrId,
                          Resource resource,
                          AbstractPcrCsvWriter csvWriter,
                          PcrTransformParams params) throws Exception {

        MedicationOrder fhir = (MedicationOrder)resource;

        long id;
        long owningOrganisationId;
        Long patientId;
        Long encounterId = null;
        Long effectivePractitionerId = null;
        Date effectiveDate = null;
        Integer effectiveDatePrecisionId = null;

        Long dmdId = null;
        String dose = null;
        BigDecimal quantityValue = null;
        String quantityUnit = null;
        Long durationDays = null;
        BigDecimal estimatedCost = null;
        Long medicationStatementId = null;

        Long conceptId = null;
//        Date insertDate = new Date();
//        Date enteredDate = null;
        Long enteredByPractitionerId = null;
        Long careActivityId = null;
        Long careActivityHeadingConceptId = -1L;
        boolean confidential = false;
        boolean isConsent = false;
        boolean isActive = false;
        Long typeConceptId = null;
        Long statusConceptId = null;
        Long medicationAmountId = null;
        Long patientInstructionsFreeTextId = null;
        Long pharmacyInstructionsFreeTextId = null;
        String originalCode =null;
        String originalTerm = null;
        Integer originalCodeScheme = null;
        Integer originalSystem = null;

        id = pcrId.longValue();
        owningOrganisationId = params.getPcrOrganisationId().longValue();
        patientId = params.getPcrPatientId();

        if (fhir.hasPrescriber()) {

            Reference practitionerReference = fhir.getPrescriber();
            effectivePractitionerId = transformOnDemandAndMapId(practitionerReference, params);
        }

//        //recorded/entered date
//        Extension enteredDateExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.RECORDED_DATE);
//        if (enteredDateExtension != null) {
//
//            DateTimeType enteredDateTimeType = (DateTimeType)enteredDateExtension.getValue();
//            enteredDate = enteredDateTimeType.getValue();
//        }

        //recorded/entered by
        Extension enteredByPractitionerExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.RECORDED_BY);
        if (enteredByPractitionerExtension != null) {

            Reference enteredByPractitionerReference = (Reference)enteredByPractitionerExtension.getValue();
            enteredByPractitionerId = transformOnDemandAndMapId(enteredByPractitionerReference, params);
        }

        //encounter / care activity
        if (fhir.hasEncounter()) {
            Reference encounterReference = fhir.getEncounter();
            encounterId = findPcrId(params, encounterReference);

            careActivityId = encounterId;            //TODO: check this is correct
        }

        if (fhir.hasDateWrittenElement()) {
            DateTimeType dt = fhir.getDateWrittenElement();
            effectiveDate = dt.getValue();
            effectiveDatePrecisionId = convertDatePrecision(dt.getPrecision());
        }

        CodeableConcept medicationCode = fhir.getMedicationCodeableConcept();
        if (medicationCode != null) {

            dmdId = CodeableConceptHelper.findSnomedConceptId(medicationCode);
            conceptId = FhirToPcrCsvTransformer.IM_PLACE_HOLDER;
                    //TODO IMClient.getConceptId(CodeScheme.SNOMED.getValue(), dmdId.toString());

        } else return;


        if (fhir.hasDosageInstruction()) {
            if (fhir.getDosageInstruction().size() > 1) {
                throw new TransformException("Cannot support MedicationStatements with more than one dose " + fhir.getId());
            }

            MedicationOrder.MedicationOrderDosageInstructionComponent doseage = fhir.getDosageInstruction().get(0);
            dose = doseage.getText();

            //one of the Emis test packs includes the unicode \u0001 character in the dose. This should be handled
            //during the inbound transform, but the data is already in the DB now, so needs handling here
            char[] chars = dose.toCharArray();
            for (int i=0; i<chars.length; i++) {
                char c = chars[i];
                if (c == 1) {
                    chars[i] = '?'; //just replace with ?
                }
            }
            dose = new String(chars);
        }

        if (fhir.hasDispenseRequest()) {
            MedicationOrder.MedicationOrderDispenseRequestComponent dispenseRequestComponent = fhir.getDispenseRequest();
            Quantity q = dispenseRequestComponent.getQuantity();
            quantityValue = q.getValue();
            quantityUnit = q.getUnit();

            if (dispenseRequestComponent.getExpectedSupplyDuration() != null) {
                Duration duration = dispenseRequestComponent.getExpectedSupplyDuration();
                if (duration != null && duration.getUnit() != null) {
                    if (!duration.getUnit().equalsIgnoreCase("days")) {
                        throw new TransformException("Unsupported medication order duration type [" + duration.getUnit() + "] for " + fhir.getId());
                    }
                    durationDays = duration.getValue().longValue();
                }
            }
        }

        if (fhir.hasStatus()) {

            MedicationOrder.MedicationOrderStatus fhirStatus = fhir.getStatus();
            isActive = (fhirStatus == MedicationOrder.MedicationOrderStatus.ACTIVE);
            statusConceptId = FhirToPcrCsvTransformer.IM_PLACE_HOLDER;
            //TODO IMClient.getConceptId("MedicationOrder.Status");
        }

        //estimated cost
        Extension estimatedCostExtension
                = ExtensionConverter.findExtension(fhir, FhirExtensionUri.MEDICATION_ORDER_ESTIMATED_COST);
        if (estimatedCost != null) {

            DecimalType d = (DecimalType)estimatedCostExtension.getValue();
            estimatedCost = d.getValue();
        }

        //medication Statement reference
        Extension medicationStatementReferenceExtension
                = ExtensionConverter.findExtension(fhir, FhirExtensionUri.MEDICATION_ORDER_AUTHORISATION);
        if (medicationStatementReferenceExtension != null) {

            Reference medicationStatementReference = (Reference) medicationStatementReferenceExtension.getValue();
            medicationStatementId = findPcrId(params, medicationStatementReference);

            //the test pack contains medication orders (i.e. issueRecords) that point to medication statements (i.e. drugRecords)
            //that don't exist, so log it out and just skip this bad record
            if (medicationStatementId == null) {
                LOG.warn("" + fhir.getResourceType() + " " + fhir.getId() + " as it refers to a MedicationStatement that doesn't exist");
            }
        }

        //auth type, i.e. acute, repeat
        Extension authorisationTypeExtension
                = ExtensionConverter.findExtension(fhir, FhirExtensionUri.MEDICATION_AUTHORISATION_TYPE);
        if (authorisationTypeExtension != null) {

            Coding c = (Coding)authorisationTypeExtension.getValue();
            MedicationAuthorisationType authorisationType = MedicationAuthorisationType.fromCode(c.getCode());
            //TODO -  not in IM yet?
            typeConceptId = FhirToPcrCsvTransformer.IM_PLACE_HOLDER;
                    //TODO IMClient.getOrCreateConceptId("MedicationAuthorisationType." + authorisationType.getCode());
        }

        //confidential?
        Extension confidentialExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.IS_CONFIDENTIAL);
        if (confidentialExtension != null) {

            BooleanType b = (BooleanType) confidentialExtension.getValue();
            confidential = b.getValue();
        }

        //unique enterprise_id values allow linkage to medication_amount table id and preserve uniqueness
        medicationAmountId = id;

        org.endeavourhealth.transform.pcr.outputModels.MedicationOrder model
                = (org.endeavourhealth.transform.pcr.outputModels.MedicationOrder)csvWriter;
        model.writeUpsert(
                id,
                patientId,
                conceptId,
                effectiveDate,
                effectiveDatePrecisionId,
                effectivePractitionerId,
                enteredByPractitionerId,
                careActivityId,
                careActivityHeadingConceptId,
                owningOrganisationId,
                statusConceptId,
                confidential,
                originalCode,
                originalTerm,
                originalCodeScheme,
                originalSystem,
                typeConceptId,
                medicationStatementId,
                medicationAmountId,
                patientInstructionsFreeTextId,
                pharmacyInstructionsFreeTextId,
                estimatedCost,
                isActive,
                durationDays,
                isConsent
        );

        //TODO - handle free text and linking

        String filename = model.getFileName();
        String idFileName = filename.replace("medicationOrder","medication_amount");
        MedicationAmount writer = new MedicationAmount(idFileName,FhirToPcrCsvTransformer.CSV_FORMAT,
                FhirToPcrCsvTransformer.DATE_FORMAT ,FhirToPcrCsvTransformer.TIME_FORMAT);

        writer.writeUpsert(
                id,
                patientId,
                dose,
                quantityValue,
                quantityUnit,
                enteredByPractitionerId);
    }
}
