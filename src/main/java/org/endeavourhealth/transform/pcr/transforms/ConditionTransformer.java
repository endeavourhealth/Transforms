package org.endeavourhealth.transform.pcr.transforms;

import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.endeavourhealth.common.fhir.schema.ProblemSignificance;
import org.endeavourhealth.transform.pcr.ObservationCodeHelper;
import org.endeavourhealth.transform.pcr.PcrTransformParams;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.endeavourhealth.transform.pcr.outputModels.Problem;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class ConditionTransformer extends AbstractTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(ConditionTransformer.class);

    public boolean shouldAlwaysTransform() {
        return true;
    }

    protected void transformResource(Long enterpriseId,
                          Resource resource,
                          AbstractPcrCsvWriter csvWriter,
                          PcrTransformParams params) throws Exception {

        Condition fhir = (Condition)resource;

        long id;

        long owningOrganisationId;
        Integer patientId;
        Long encounterId = null;
        Date effectiveDate = null;
        Integer effectiveDatePrecisionId = null;
        Long snomedConceptId = null;
        String originalCode = null;
        boolean isProblem = false;
        String originalTerm = null;
        boolean isReview = false;
        Date problemEndDate = null;
        Long parentObservationId = null;

        Long observationId = null;   //TODO - how derive this?

        Integer conceptId = null;
        Date insertDate = new Date();
        Date enteredDate = null;
        Integer effectivePractitionerId = null;
        Long careActivityId = null;
        Integer careActivityHeadingConceptId = null;
        Integer statusConceptId = null;
        boolean confidential = false;
        Integer episodicityConceptId = null;
        Long freeTextId = null;
        Integer dataEntryPromptId = null;
        Integer significanceConceptId = null;
        boolean isConsent = false;
        Integer expectedDurationDays = null;
        Date lastReviewDate = null;
        Integer enteredByPractitionerId = null;
        Integer lastReviewPractitionerId = null;
        Integer typeConceptId = null;


        id = enterpriseId.longValue();
        owningOrganisationId = params.getEnterpriseOrganisationId().longValue();
        patientId = params.getEnterprisePatientId().intValue();
        //personId = params.getEnterprisePersonId().longValue();   //TODO: track down source and remove for PCR

        if (fhir.hasEncounter()) {
            Reference encounterReference = fhir.getEncounter();
            encounterId = findEnterpriseId(params, encounterReference);

            careActivityId = encounterId;   //TODO: check this is correct
        }

        if (fhir.hasAsserter()) {
            Reference practitionerReference = fhir.getAsserter();
            effectivePractitionerId = transformOnDemandAndMapId(practitionerReference, params).intValue();
        }

        if (fhir.hasOnsetDateTimeType()) {
            DateTimeType dt = fhir.getOnsetDateTimeType();
            effectiveDate = dt.getValue();
            effectiveDatePrecisionId = convertDatePrecision(dt.getPrecision());
        }

        ObservationCodeHelper codes = ObservationCodeHelper.extractCodeFields(fhir.getCode());
        if (codes != null) {

            snomedConceptId = codes.getSnomedConceptId();
            originalCode = codes.getOriginalCode();
            originalTerm = codes.getOriginalTerm();

            //TODO: map to IM conceptId
            //conceptId = ??
        }


        //if it's a problem set the boolean to say so
        if (fhir.hasMeta()) {
            for (UriType uriType: fhir.getMeta().getProfile()) {
                if (uriType.getValue().equals(FhirProfileUri.PROFILE_URI_PROBLEM)) {
                    isProblem = true;
                }
            }
        }

        if (fhir.hasAbatement()
                && fhir.getAbatement() instanceof DateType) {
            DateType dateType = (DateType)fhir.getAbatement();
            problemEndDate = dateType.getValue();
        }

        Extension reviewExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.IS_REVIEW);
        if (reviewExtension != null) {
            BooleanType b = (BooleanType)reviewExtension.getValue();
            if (b.getValue() != null) {
                isReview = b.getValue();
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

        Extension problemLastReviewDateExtension
                = ExtensionConverter.findExtension(fhir, FhirExtensionUri.PROBLEM_LAST_REVIEWED);
        if (problemLastReviewDateExtension != null) {

            DateType problemLastReviewDateExtensionType = (DateType) problemLastReviewDateExtension.getValue();
            lastReviewDate = problemLastReviewDateExtensionType.getValue();
        }

        //TODO - lastReviewPractitionerId

        Extension problemExpectedDurationExtension
                = ExtensionConverter.findExtension(fhir, FhirExtensionUri.PROBLEM_EXPECTED_DURATION);
        if (problemExpectedDurationExtension != null) {

            IntegerType problemExpectedDurationExtensionType = (IntegerType) problemExpectedDurationExtension.getValue();
            expectedDurationDays = problemExpectedDurationExtensionType.getValue();
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

        Extension parentExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.PARENT_RESOURCE);
        if (parentExtension != null) {
            Reference parentReference = (Reference)parentExtension.getValue();
            parentObservationId = findEnterpriseId(params, parentReference);
        }

        //TODO - typeConceptId

        //firstly, file as an observation
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

        //then, if it is a problem, file into problem using id as observationId?
        if (isProblem) {
            Problem problemModel = (Problem)csvWriter;

            problemModel.writeUpsert(
                    id,
                    patientId,
                    observationId,    //TODO: how derive this?
                    typeConceptId,
                    significanceConceptId,
                    expectedDurationDays,
                    lastReviewDate,
                    lastReviewPractitionerId
            );
        }

        //TODO - handle free text and linking
    }
}
