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

public class MedicationStatementTransformer extends AbstractTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(MedicationStatementTransformer.class);

    public boolean shouldAlwaysTransform() {
        return true;
    }

    protected void transformResource(Long pcrId,
                          Resource resource,
                          AbstractPcrCsvWriter csvWriter,
                          PcrTransformParams params) throws Exception {

        MedicationStatement fhir = (MedicationStatement)resource;

        long id;
        long owningOrganisationId;
        long patientId;
        Long encounterId = null;
        Long effectivePractitionerId = null;
        Date effectiveDate = null;
        Integer effectiveDatePrecisionId = null;

        Long dmdId = null;
        Boolean isActive = null;
        Date cancellationDate = null;
        String dose = null;
        BigDecimal quantityValue = null;
        String quantityUnit = null;

        Long conceptId = null;
        Date insertDate = new Date();
        Date enteredDate = null;
        Long enteredByPractitionerId = null;
        Long careActivityId = null;
        Long careActivityHeadingConceptId = -1L;
        Long statusConceptId = null;
        boolean confidential = false;
        Long typeConceptId = null;
        boolean isConsent = false;
        Integer issues = null;
        Integer issuesAuthorised = null;

        Long patientInstructionsFreeTextId = null;
        Long pharmacyInstructionsFreeTextId = null;
        Date reviewDate = null;  //not supported in FHIR?
        Long endReasonConceptId = null;
        Long endReasonFreeTextId = null;
        Long medicationAmountId = null;
        Integer courseLengthPerIssueDays = null;
        String originalCode = null;
        String originalTerm = null;
        Integer originalCodeScheme = null;
        Integer originalSystem = null;

        id = pcrId.longValue();
        owningOrganisationId = params.getPcrOrganisationId().longValue();
        patientId = params.getPcrPatientId().intValue();

        if (fhir.hasInformationSource()) {
            Reference practitionerReference = fhir.getInformationSource();
            effectivePractitionerId = transformOnDemandAndMapId(practitionerReference, params);
        }

        if (fhir.hasDateAssertedElement()) {
            DateTimeType dt = fhir.getDateAssertedElement();
            effectiveDate = dt.getValue();
            effectiveDatePrecisionId = convertDatePrecision(dt.getPrecision());
        }

        //recorded/entered date
        Extension enteredDateExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.RECORDED_DATE);
        if (enteredDateExtension != null) {

            DateTimeType enteredDateTimeType = (DateTimeType)enteredDateExtension.getValue();
            enteredDate = enteredDateTimeType.getValue();
        }

        //recorded/entered by
        Extension enteredByPractitionerExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.RECORDED_BY);
        if (enteredByPractitionerExtension != null) {

            Reference enteredByPractitionerReference = (Reference)enteredByPractitionerExtension.getValue();
            enteredByPractitionerId = transformOnDemandAndMapId(enteredByPractitionerReference, params);
        }

        //encounter / care activity
        Extension encounterExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.ASSOCIATED_ENCOUNTER);
        if (encounterExtension != null) {

            Reference encounterReference = (Reference)encounterExtension.getValue();
            encounterId = findPcrId(params, encounterReference);

            careActivityId = encounterId;            //TODO: check this is correct
        }


        CodeableConcept medicationCode = fhir.getMedicationCodeableConcept();
        if (medicationCode != null) {

            dmdId = CodeableConceptHelper.findSnomedConceptId(medicationCode);
            conceptId = FhirToPcrCsvTransformer.IM_PLACE_HOLDER;
                    //TODO IMClient.getConceptId(CodeScheme.SNOMED.getValue(), dmdId.toString());

        } else return;


        if (fhir.hasStatus()) {

            MedicationStatement.MedicationStatementStatus fhirStatus = fhir.getStatus();
            isActive = (fhirStatus == MedicationStatement.MedicationStatementStatus.ACTIVE);
            statusConceptId = FhirToPcrCsvTransformer.IM_PLACE_HOLDER;
        //TODO IMClient.getConceptId("MedicationStatementStatus." + fhirStatus.toCode());
        }

        if (fhir.hasDosage()) {
            if (fhir.getDosage().size() > 1) {
                throw new TransformException("Cannot support MedicationStatements with more than one dose " + fhir.getId());
            }

            MedicationStatement.MedicationStatementDosageComponent doseage = fhir.getDosage().get(0);
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

        Extension cancellationExtension
                = ExtensionConverter.findExtension(fhir, FhirExtensionUri.MEDICATION_AUTHORISATION_CANCELLATION);
        if (cancellationExtension != null) {
            if (cancellationExtension.hasExtension()) {
                for (Extension innerExtension: cancellationExtension.getExtension()) {
                    if (innerExtension.getValue() instanceof DateType) {
                        DateType d = (DateType)innerExtension.getValue();
                        cancellationDate = d.getValue();
                    }
                }
            }
        }

        //quantity
        Extension authorisedQtyExtension
                = ExtensionConverter.findExtension(fhir, FhirExtensionUri.MEDICATION_AUTHORISATION_QUANTITY);
        if (authorisedQtyExtension != null) {

            Quantity q = (Quantity)authorisedQtyExtension.getValue();
            quantityValue = q.getValue();
            quantityUnit = q.getUnit();
        }

        //auth type, i.e. acute, repeat
        Extension authorisationTypeExtension
                = ExtensionConverter.findExtension(fhir, FhirExtensionUri.MEDICATION_AUTHORISATION_TYPE);
        if (authorisationTypeExtension != null) {

            Coding c = (Coding)authorisationTypeExtension.getValue();
            MedicationAuthorisationType authorisationType = MedicationAuthorisationType.fromCode(c.getCode());
            typeConceptId = FhirToPcrCsvTransformer.IM_PLACE_HOLDER;
                    //TODO IMClient.getConceptId("MedicationAuthorisationType." + authorisationType.getCode());
        }

        //issues authorised
        Extension numberOfIssuesAuthorisedExtension
                = ExtensionConverter.findExtension(fhir, FhirExtensionUri.MEDICATION_AUTHORISATION_NUMBER_OF_REPEATS_ALLOWED);
        if (numberOfIssuesAuthorisedExtension != null) {

            IntegerType issuesAllowedType = (IntegerType)numberOfIssuesAuthorisedExtension.getValue();
            issuesAuthorised = issuesAllowedType.getValue();
        }

        //issues issued
        Extension numberOfIssuesExtension
                = ExtensionConverter.findExtension(fhir, FhirExtensionUri.MEDICATION_AUTHORISATION_NUMBER_OF_REPEATS_ISSUED);
        if (numberOfIssuesExtension != null) {

            IntegerType issuesIssuedType = (IntegerType)numberOfIssuesExtension.getValue();
            issues = issuesIssuedType.getValue();
        }

        //confidential?
        Extension confidentialExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.IS_CONFIDENTIAL);
        if (confidentialExtension != null) {

            BooleanType b = (BooleanType) confidentialExtension.getValue();
            confidential = b.getValue();
        }

        //unique enterprise_id values allow linkage to medication_amount table id and preserve uniqueness
        medicationAmountId = id;

        org.endeavourhealth.transform.pcr.outputModels.MedicationStatement model
                = (org.endeavourhealth.transform.pcr.outputModels.MedicationStatement)csvWriter;

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
                medicationAmountId,
                issuesAuthorised,
                reviewDate,                         //not available in FHIR
                courseLengthPerIssueDays,         //medicationOrder - courseDuration
                patientInstructionsFreeTextId,
                pharmacyInstructionsFreeTextId,
                isActive,
                cancellationDate,
                endReasonConceptId,         //not available
                endReasonFreeTextId,        //not available
                issues,
                isConsent);


        //TODO - handle free text and linking

        String filename = model.getFileName();
        String idFileName = filename.replace("medication_statement","medication_amount");
        MedicationAmount medicationAmountModel = new MedicationAmount(idFileName,FhirToPcrCsvTransformer.CSV_FORMAT,
                FhirToPcrCsvTransformer.DATE_FORMAT ,FhirToPcrCsvTransformer.TIME_FORMAT);


        medicationAmountModel.writeUpsert(
                id,
                patientId,
                dose,
                quantityValue,
                quantityUnit,
                enteredByPractitionerId);

    }
}

