package org.endeavourhealth.transform.pcr.transforms;

import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.pcr.ObservationCodeHelper;
import org.endeavourhealth.transform.pcr.PcrTransformParams;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
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
        Integer conceptId = null;
        BigDecimal resultValue = null;
        String resultValueUnits = null;
        Date resultDate = null;
        String resultText = null;
        Long resultSnomedConceptId = null;
        String originalCode = null;
        String originalTerm = null;

        //TODO - this lot need assigning for observation
        Date insertDate = null;
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

        //TODO - this lot need assigning for observation value
        Integer operatorConceptId = null;
        Long referenceRangeId = null;

        id = enterpriseId.longValue();
        owningOrganisationId = params.getEnterpriseOrganisationId().intValue();
        patientId = params.getEnterprisePatientId().intValue();

        if (fhir.hasEncounter()) {
            Reference encounterReference = fhir.getEncounter();
            encounterId = findEnterpriseId(params, encounterReference);  //TODO - CareActivityId ?
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
        if (codes == null) {
            return;
        }
        snomedConceptId = codes.getSnomedConceptId();
        //TODO: map to IM conceptId
        //conceptId = ??

        originalCode = codes.getOriginalCode();
        originalTerm = codes.getOriginalTerm();

        if (fhir.hasValue()) {
            Type value = fhir.getValue();
            if (value instanceof Quantity) {
                Quantity quantity = (Quantity) value;
                resultValue = quantity.getValue();
                resultValueUnits = quantity.getUnit();

            } else if (value instanceof DateTimeType) {
                DateTimeType dateTimeType = (DateTimeType) value;
                resultDate = dateTimeType.getValue();

            } else if (value instanceof StringType) {
                StringType stringType = (StringType) value;
                resultText = stringType.getValue();

            } else if (value instanceof CodeableConcept) {
                CodeableConcept resultCodeableConcept = (CodeableConcept) value;
                resultSnomedConceptId = CodeableConceptHelper.findSnomedConceptId(resultCodeableConcept);
                //TODO: map to IM conceptId
                //resultConceptId = ??

            } else {
                throw new TransformException("Unsupported value type " + value.getClass() + " for " + fhir.getResourceType() + " " + fhir.getId());
            }
        }


        //TODO - how deal with this problem data, from Condition?
//        Extension reviewExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.IS_REVIEW);
//        if (reviewExtension != null) {
//            BooleanType b = (BooleanType) reviewExtension.getValue();
//            if (b.getValue() != null) {
//                isReview = b.getValue();
//            }
//        }
//
//        Extension parentExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.PARENT_RESOURCE);
//        if (parentExtension != null) {
//            Reference parentReference = (Reference) parentExtension.getValue();
//            parentObservationId = findEnterpriseId(params, parentReference);
//        }

        org.endeavourhealth.transform.pcr.outputModels.Observation observationModel
                = (org.endeavourhealth.transform.pcr.outputModels.Observation) csvWriter;
        observationModel.writeUpsert(id,
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


        org.endeavourhealth.transform.pcr.outputModels.ObservationValue observationValueModel
                = (org.endeavourhealth.transform.pcr.outputModels.ObservationValue) csvWriter;

        observationValueModel.writeUpsert(patientId,
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
}



