package org.endeavourhealth.transform.pcr.transforms;

import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.ProblemSignificance;
import org.endeavourhealth.im.client.IMClient;
import org.endeavourhealth.im.models.CodeScheme;
import org.endeavourhealth.transform.pcr.ObservationCodeHelper;
import org.endeavourhealth.transform.pcr.PcrTransformParams;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.endeavourhealth.transform.pcr.outputModels.OutputModelsFromEnterprise.ObservationValue;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.crypto.dsig.TransformException;
import java.math.BigDecimal;
import java.util.Date;

public class ObservationTransformer extends AbstractTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(ObservationTransformer.class);

    public boolean shouldAlwaysTransform() {
        return true;
    }

    protected void transformResource(Long pcrId,
                          Resource resource,
                          AbstractPcrCsvWriter csvWriter,
                          PcrTransformParams params) throws Exception {

        Observation fhir = (Observation) resource;

        long id;
        Long owningOrganisationId;
        Integer patientId;

        Long encounterId = null;
        Integer effectivePractitionerId = null;
        Date effectiveDate = null;
        Integer effectiveDatePrecisionId = null;
        Long snomedConceptId = null;

        BigDecimal resultValue = null;
        String resultValueUnits = null;
        Date resultDate = null;
        String resultText = null;
        Long resultSnomedConceptId = null;
        String originalCode = null;
        String originalTerm = null;

        Long conceptId = null;
        Date insertDate = new Date();
        Date enteredDate = null;
        Integer enteredByPractitionerId = null;
        Long careActivityId = null;
        Long careActivityHeadingConceptId = null;
        Long statusConceptId = null;
        boolean confidential = false;
        Long episodicityConceptId = null;
        Long freeTextId = null;
        Integer dataEntryPromptId = null;
        Long significanceConceptId = null;
        boolean isConsent = false;
        Long resultConceptId = null;
        Long operatorConceptId = null;
        Long referenceRangeId = null;

        id = pcrId.longValue();
        owningOrganisationId = params.getEnterpriseOrganisationId().longValue();
        patientId = params.getEnterprisePatientId().intValue();

        if (fhir.hasEncounter()) {

            Reference encounterReference = fhir.getEncounter();
            encounterId = findEnterpriseId(params, encounterReference);

            careActivityId = encounterId;            //TODO: check this is correct
        }

        if (fhir.hasPerformer()) {

            for (Reference reference : fhir.getPerformer()) {
                ResourceType resourceType = ReferenceHelper.getResourceType(reference);
                if (resourceType == ResourceType.Practitioner) {
                    effectivePractitionerId = transformOnDemandAndMapId(reference, params).intValue();
                }
            }
        }

        if (fhir.hasEffectiveDateTimeType()) {

            DateTimeType dt = fhir.getEffectiveDateTimeType();
            effectiveDate = dt.getValue();
            effectiveDatePrecisionId = convertDatePrecision(dt.getPrecision());
        }

        ObservationCodeHelper codes = ObservationCodeHelper.extractCodeFields(fhir.getCode());
        if (codes != null) {

            snomedConceptId = codes.getSnomedConceptId();
            conceptId = IMClient.getConceptId(CodeScheme.SNOMED.getValue(), snomedConceptId.toString());

            originalCode = codes.getOriginalCode();
            originalTerm = codes.getOriginalTerm();
        } else return;

        if (fhir.hasValue()) {

            Type value = fhir.getValue();
            if (value instanceof Quantity) {
                Quantity quantity = (Quantity) value;
                resultValue = quantity.getValue();
                resultValueUnits = quantity.getUnit();

                Quantity.QuantityComparator comparator = quantity.getComparator();
                operatorConceptId = IMClient.getOrCreateConceptId("Quantity.QuantityComparator." + comparator.toCode());

            } else if (value instanceof DateTimeType) {
                DateTimeType dateTimeType = (DateTimeType) value;
                resultDate = dateTimeType.getValue();

            } else if (value instanceof StringType) {
                StringType stringType = (StringType) value;
                resultText = stringType.getValue();

            } else if (value instanceof CodeableConcept) {
                CodeableConcept resultCodeableConcept = (CodeableConcept) value;
                resultSnomedConceptId = CodeableConceptHelper.findSnomedConceptId(resultCodeableConcept);
                resultConceptId = IMClient.getConceptId(CodeScheme.SNOMED.getValue(), resultSnomedConceptId.toString());

            } else {
                throw new TransformException("Unsupported value type " + value.getClass() + " for " + fhir.getResourceType() + " " + fhir.getId());
            }
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
            enteredByPractitionerId = transformOnDemandAndMapId(enteredByPractitionerReference, params).intValue();
        }

        //TODO: where get heading from?
        //careActivityHeadingConceptId

        //observation status
        if (fhir.hasStatus()) {
            statusConceptId = IMClient.getOrCreateConceptId("ObservationStatus." +fhir.getStatus().toCode());
        }

        //confidential?
        Extension confidentialExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.IS_CONFIDENTIAL);
        if (confidentialExtension != null) {

            BooleanType b = (BooleanType) confidentialExtension.getValue();
            confidential = b.getValue();
        }

        Extension episodicityExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.PROBLEM_EPISODICITY);
        if (episodicityExtension != null) {

            StringType episodicityType = (StringType) episodicityExtension.getValue();
            episodicityConceptId
                    = IMClient.getConceptId("FhirExtensionUri.PROBLEM_EPISODICITY");
            //TODO do we know how extension uri is mapped?
        }

        Extension significanceExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.PROBLEM_SIGNIFICANCE);
        if (significanceExtension != null) {

            CodeableConcept codeableConcept = (CodeableConcept)significanceExtension.getValue();
            ProblemSignificance fhirSignificance = ProblemSignificance.fromCodeableConcept(codeableConcept);

            significanceConceptId =  IMClient.getConceptId(CodeScheme.SNOMED.getValue(),fhirSignificance.getCode());
            //TODO not sure how we model these codeschemes yet
        }

        //referenceRangeId = ??  //TODO: map to IM concept (not set in FHIR)

        org.endeavourhealth.transform.pcr.outputModels.OutputModelsFromEnterprise.Observation observationModel
                = (org.endeavourhealth.transform.pcr.outputModels.OutputModelsFromEnterprise.Observation) csvWriter;
        observationModel.writeUpsert(
                id,
                patientId,
                conceptId,
                effectiveDate,
                effectiveDatePrecisionId,
                effectivePractitionerId,
                careActivityId,
                careActivityHeadingConceptId,
                owningOrganisationId,
                confidential,
                originalCode,
                originalTerm,
                episodicityConceptId,
                freeTextId,
                dataEntryPromptId,
                significanceConceptId,
                isConsent);


        //if the observation has a value then file that data
        if (fhir.hasValue()) {

            ObservationValue observationValueModel = (ObservationValue) csvWriter;
            observationValueModel.writeUpsert(
                    patientId,
                    id,
                    operatorConceptId,
                    resultValue,
                    resultValueUnits,
                    resultDate,
                    resultText,
                    resultConceptId,
                    referenceRangeId
            );
        }
        //TODO is this where we get allergy data from?

        //TODO - handle free text and linking
    }
}



