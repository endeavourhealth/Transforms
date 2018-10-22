package org.endeavourhealth.transform.pcr.transforms;

import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.fhir.schema.ProblemSignificance;
import org.endeavourhealth.transform.pcr.ObservationCodeHelper;
import org.endeavourhealth.transform.pcr.PcrTransformParams;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.endeavourhealth.transform.pcr.outputModels.ObservationValue;
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

    protected void transformResource(Long enterpriseId,
                          Resource resource,
                          AbstractPcrCsvWriter csvWriter,
                          PcrTransformParams params) throws Exception {

        Observation fhir = (Observation) resource;

        long id;
        Integer owningOrganisationId;
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

        Integer conceptId = null;
        Date insertDate = new Date();
        Date enteredDate = null;
        Integer enteredByPractitionerId = null;
        Long careActivityId = null;
        Integer careActivityHeadingConceptId = null;
        Integer statusConceptId = null;
        boolean confidential = false;
        Integer episodicityConceptId = null;
        Long freeTextId = null;
        Integer dataEntryPromptId = null;
        Integer significanceConceptId = null;
        boolean isConsent = false;
        Integer resultConceptId = null;
        Integer operatorConceptId = null;
        Long referenceRangeId = null;

        id = enterpriseId.longValue();
        owningOrganisationId = params.getEnterpriseOrganisationId().intValue();
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
            //TODO: map to IM conceptId
            //conceptId = ??

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
                //operatorConceptId = ??  //TODO: map to IM conceptId

            } else if (value instanceof DateTimeType) {
                DateTimeType dateTimeType = (DateTimeType) value;
                resultDate = dateTimeType.getValue();

            } else if (value instanceof StringType) {
                StringType stringType = (StringType) value;
                resultText = stringType.getValue();

            } else if (value instanceof CodeableConcept) {
                CodeableConcept resultCodeableConcept = (CodeableConcept) value;
                resultSnomedConceptId = CodeableConceptHelper.findSnomedConceptId(resultCodeableConcept);

                //resultConceptId = ??  //TODO: map to IM conceptId

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

        //immunisation status
        if (fhir.hasStatus()) {

            Observation.ObservationStatus status = fhir.getStatus();
            //statusConceptId = ??    //TODO: map to IM concept
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
            String episodicity = episodicityType.getValue();  //TODO: map to IM concept
        }

        Extension significanceExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.PROBLEM_SIGNIFICANCE);
        if (significanceExtension != null) {

            CodeableConcept codeableConcept = (CodeableConcept)significanceExtension.getValue();
            ProblemSignificance fhirSignificance = ProblemSignificance.fromCodeableConcept(codeableConcept);

            //significanceConceptId = ??  //TODO: map to IM concept
        }

        //referenceRangeId = ??  //TODO: map to IM concept

        org.endeavourhealth.transform.pcr.outputModels.Observation observationModel
                = (org.endeavourhealth.transform.pcr.outputModels.Observation) csvWriter;
        observationModel.writeUpsert(
                id,
                patientId,
                conceptId,
                effectiveDate,
                effectiveDatePrecisionId,
                effectivePractitionerId,
                insertDate,
                enteredDate,
                enteredByPractitionerId,
                careActivityId,
                careActivityHeadingConceptId,
                owningOrganisationId,
                statusConceptId,
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


        //TODO - handle free text and linking
    }
}



