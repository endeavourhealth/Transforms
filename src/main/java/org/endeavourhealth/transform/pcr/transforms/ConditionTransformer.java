package org.endeavourhealth.transform.pcr.transforms;

import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.endeavourhealth.common.fhir.schema.ProblemSignificance;
import org.endeavourhealth.transform.pcr.FhirToPcrCsvTransformer;
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

    protected void transformResource(Long pcrId,
                          Resource resource,
                          AbstractPcrCsvWriter csvWriter,
                          PcrTransformParams params) throws Exception {

        Condition fhir = (Condition)resource;

        long id;

        Long owningOrganisationId;
        Long patientId;
        Long encounterId = null;
        Date effectiveDate = null;
        Integer effectiveDatePrecisionId = null;
        Long snomedConceptId = null;
        String originalCode = null;
        boolean isProblem = false;
        String originalTerm = null;
        Integer originalCodeScheme= null;
        Integer originalSystem = null;
        boolean isReview = false;
        Date problemEndDate = null;
        Long parentObservationId = null;

        Long observationId = null;

        Long conceptId = null;
        Date insertDate = new Date();
        Date enteredDate = null;
        Long effectivePractitionerId = null;
        Long careActivityId = null;
        Long careActivityHeadingConceptId = -1L;
        Long statusConceptId = null;  //not available in FHIR
        boolean confidential = false;
        Long episodicityConceptId = null;
        Long freeTextId = null;
        Long dataEntryPromptId = null;
        Long significanceConceptId = null;
        boolean isConsent = false;
        Integer expectedDurationDays = null;
        Date lastReviewDate = null;
        Long enteredByPractitionerId = null;
        Long lastReviewPractitionerId = null;
        Long typeConceptId = null;

        id = pcrId.longValue();
        owningOrganisationId = params.getPcrOrganisationId().longValue();
        patientId = params.getPcrPatientId();

        if (fhir.hasEncounter()) {
            Reference encounterReference = fhir.getEncounter();
            encounterId = findPcrId(params, encounterReference);

            careActivityId = encounterId;   //TODO: check this is correct
        }

        if (fhir.hasAsserter()) {
            Reference practitionerReference = fhir.getAsserter();
            effectivePractitionerId = transformOnDemandAndMapId(practitionerReference, params);
        }

        if (fhir.hasOnsetDateTimeType()) {
            DateTimeType dt = fhir.getOnsetDateTimeType();
            effectiveDate = dt.getValue();
            effectiveDatePrecisionId = convertDatePrecision(dt.getPrecision());
        }

        ObservationCodeHelper codes = ObservationCodeHelper.extractCodeFields(fhir.getCode());
        if (codes != null) {

            snomedConceptId = codes.getSnomedConceptId();
            //TODO conceptId = IMClient.getConceptId(CodeScheme.SNOMED.getValue(), snomedConceptId.toString());
            conceptId = FhirToPcrCsvTransformer.IM_PLACE_HOLDER;

            originalCode = codes.getOriginalCode();
            originalTerm = codes.getOriginalTerm();

        } else return;


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
            enteredByPractitionerId = transformOnDemandAndMapId(enteredByPractitionerReference, params);
        }

        //last review date and by which practitioner, a compound extension
        Extension problemLastReviewedExtension
                = ExtensionConverter.findExtension(fhir, FhirExtensionUri.PROBLEM_LAST_REVIEWED);
        if (problemLastReviewedExtension != null) {

            Extension problemLastReviewByExtension
                    = ExtensionConverter.findExtension(problemLastReviewedExtension, FhirExtensionUri._PROBLEM_LAST_REVIEWED__PERFORMER);
            if (problemLastReviewByExtension != null) {

                Reference lastReviewPractitionerReference = (Reference) problemLastReviewByExtension.getValue();
                lastReviewPractitionerId = transformOnDemandAndMapId(lastReviewPractitionerReference, params);
            }

            Extension problemLastReviewedDateExtension
                    = ExtensionConverter.findExtension(problemLastReviewedExtension, FhirExtensionUri._PROBLEM_LAST_REVIEWED__DATE);
            if (problemLastReviewedDateExtension != null) {

                DateType problemLastReviewDateExtensionType = (DateType) problemLastReviewedDateExtension.getValue();
                lastReviewDate = problemLastReviewDateExtensionType.getValue();
            }
        }

        Extension problemExpectedDurationExtension
                = ExtensionConverter.findExtension(fhir, FhirExtensionUri.PROBLEM_EXPECTED_DURATION);
        if (problemExpectedDurationExtension != null) {

            IntegerType problemExpectedDurationExtensionType = (IntegerType) problemExpectedDurationExtension.getValue();
            expectedDurationDays = problemExpectedDurationExtensionType.getValue();
        }

        Extension episodicityExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.PROBLEM_EPISODICITY);
        if (episodicityExtension != null) {

            StringType episodicityType = (StringType) episodicityExtension.getValue();
            episodicityConceptId = FhirToPcrCsvTransformer.IM_PLACE_HOLDER;
             //TODO       = IMClient.getConceptId("FhirExtensionUri.PROBLEM_EPISODICITY");
            //TODO do we know how these URIs are mapped yet in IM?
        }

        Extension significanceExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.PROBLEM_SIGNIFICANCE);
        if (significanceExtension != null) {

            CodeableConcept codeableConcept = (CodeableConcept)significanceExtension.getValue();
            ProblemSignificance fhirSignificance = ProblemSignificance.fromCodeableConcept(codeableConcept);

            significanceConceptId = FhirToPcrCsvTransformer.IM_PLACE_HOLDER;
              //TODO      IMClient.getConceptId(CodeScheme.SNOMED.getValue(),fhirSignificance.getCode());
        }

        Extension parentExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.PARENT_RESOURCE);
        if (parentExtension != null) {
            Reference parentReference = (Reference)parentExtension.getValue();
            parentObservationId = findPcrId(params, parentReference);

            //TODO:// EventRelationship
        }

        CodeableConcept conditionCategory = fhir.getCategory();
        if (conditionCategory != null) {

            String categoryType = conditionCategory.getCoding().get(0).getCode();
            typeConceptId =
                    FhirToPcrCsvTransformer.IM_PLACE_HOLDER;
            //TODO IMClient.getOrCreateConceptId("Condition.category." + categoryType);
        }

        //firstly, file as an observation
        Problem problemModel
                = (Problem) csvWriter;
        problemModel.writeUpsert(
                id,
                patientId,
                parentObservationId,
                typeConceptId,
                significanceConceptId,
                expectedDurationDays,
                lastReviewDate,
                lastReviewPractitionerId,
                enteredByPractitionerId);



        //TODO - handle free text and linking
    }
}
