package org.endeavourhealth.transform.subscriber.transforms;

import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.im.client.IMClient;
import org.endeavourhealth.transform.common.TransformConfig;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.enterprise.ObservationCodeHelper;
import org.endeavourhealth.transform.subscriber.IMConstant;
import org.endeavourhealth.transform.subscriber.IMHelper;
import org.endeavourhealth.transform.subscriber.SubscriberTransformHelper;
import org.endeavourhealth.transform.subscriber.targetTables.ObservationAdditional;
import org.endeavourhealth.transform.subscriber.targetTables.OutputContainer;
import org.endeavourhealth.transform.subscriber.targetTables.SubscriberTableId;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Date;

public class ConditionTransformer extends AbstractSubscriberTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(ConditionTransformer.class);

    @Override
    protected ResourceType getExpectedResourceType() {
        return ResourceType.Condition;
    }

    public boolean shouldAlwaysTransform() {
        return true;
    }

    @Override
    protected void transformResource(SubscriberId subscriberId, ResourceWrapper resourceWrapper, SubscriberTransformHelper params) throws Exception {

        org.endeavourhealth.transform.subscriber.targetTables.Observation model = params.getOutputContainer().getObservations();

        org.endeavourhealth.transform.subscriber.targetTables.ObservationAdditional additionalModel = params.getOutputContainer().getObservationAdditional();

        Condition fhir = (Condition)resourceWrapper.getResource(); //returns null if deleted

        //if deleted, confidential or the entire patient record shouldn't be there, then delete
        if (resourceWrapper.isDeleted()
                //|| isConfidential(fhir)
                || params.getShouldPatientRecordBeDeleted()
                || params.shouldClinicalConceptBeDeleted(fhir.getCode())) {

            if (!TransformConfig.instance().isLive()) {
                additionalModel.writeDelete(subscriberId);
            }
            model.writeDelete(subscriberId);
            return;
        }

        long organizationId;
        long patientId;
        long personId;
        Long encounterId = null;
        Long practitionerId = null;
        Date clinicalEffectiveDate = null;
        Integer datePrecisionConceptId = null;
        // Long snomedConceptId = null;
        BigDecimal resultValue = null;
        String resultValueUnits = null;
        Date resultDate = null;
        String resultString = null;
        Long resultConceptId = null;
        boolean isProblem = false;
        boolean isReview = false;
        Date problemEndDate = null;
        Long parentObservationId = null;
        Integer coreConceptId = null;
        Integer nonCoreConceptId = null;
        Double ageAtEvent = null;
        Integer episodicityConceptId = null;
        Boolean isPrimary = null;
        Date dateRecorded = null;

        organizationId = params.getSubscriberOrganisationId().longValue();
        patientId = params.getSubscriberPatientId().longValue();
        personId = params.getSubscriberPersonId().longValue();

        if (fhir.hasEncounter()) {
            Reference encounterReference = fhir.getEncounter();
            encounterId = transformOnDemandAndMapId(encounterReference, SubscriberTableId.ENCOUNTER, params);
        }

        if (fhir.hasAsserter()) {
            Reference practitionerReference = fhir.getAsserter();
            practitionerId = transformOnDemandAndMapId(practitionerReference, SubscriberTableId.PRACTITIONER, params);
        }

        if (fhir.hasOnsetDateTimeType()) {
            DateTimeType dt = fhir.getOnsetDateTimeType();
            clinicalEffectiveDate = dt.getValue();
            datePrecisionConceptId = convertDatePrecision(params, fhir, dt.getPrecision(),clinicalEffectiveDate.toString());
        }

        Coding originalCoding = ObservationCodeHelper.findOriginalCoding(fhir.getCode());
        if (originalCoding == null) {
            TransformWarnings.log(LOG, params, "No suitable Coding found for {} {}", fhir.getResourceType(), fhir.getId());
            return;
        }
        String originalCode = originalCoding.getCode();

        String conceptScheme = ObservationCodeHelper.mapCodingSystemToImScheme(originalCoding);
        coreConceptId = IMHelper.getIMMappedConcept(params, fhir, conceptScheme, originalCode);
        nonCoreConceptId = IMHelper.getIMConcept(params, fhir, conceptScheme, originalCode, originalCoding.getDisplay());


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

        parentObservationId = ObservationTransformer.transformParentResourceReference(fhir, params);

        if (fhir.getPatient() != null) {
            Reference ref = fhir.getPatient();
            Patient patient = params.getCachedPatient(ref);
            ageAtEvent = getPatientAgeInDecimalYears(patient, clinicalEffectiveDate);
        }

        Extension episodicityExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.PROBLEM_EPISODICITY);
        if (episodicityExtension != null) {
            StringType episodicity = (StringType) episodicityExtension.getValue();
            episodicityConceptId = IMHelper.getIMConcept(params, fhir, IMConstant.FHIR_CONDITION_EPISODICITY,
                    episodicity.getValue(), episodicity.getValue());
        }

        Extension isPrimaryExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.IS_PRIMARY);
        if (isPrimaryExtension != null) {
            BooleanType b = (BooleanType)isPrimaryExtension.getValue();
            if (b.getValue() != null) {
                isPrimary = b.getValue();
            }
        }

        dateRecorded = params.includeDateRecorded(fhir);

        model.setIncludeDateRecorded(params.isIncludeDateRecorded());
        model.writeUpsert(subscriberId,
                organizationId,
                patientId,
                personId,
                encounterId,
                practitionerId,
                clinicalEffectiveDate,
                datePrecisionConceptId,
                resultValue,
                resultValueUnits,
                resultDate,
                resultString,
                resultConceptId,
                isProblem,
                isReview,
                problemEndDate,
                parentObservationId,
                coreConceptId,
                nonCoreConceptId,
                ageAtEvent,
                episodicityConceptId,
                isPrimary,
                dateRecorded);

        //we also need to populate the observation additional table with observation extension data

        if (!TransformConfig.instance().isLive()) {
            transformAdditionals(fhir, params, subscriberId);
        }

    }

    private void transformAdditionals(Resource resource, SubscriberTransformHelper params, SubscriberId id) throws Exception {

        Condition fhir = (Condition)resource;

        //if it has no extension data, then nothing further to do
        if (!fhir.hasExtension()) {
            return;
        }

        String significanceDisplay = null;

        //Process the problem significance
        Extension significanceExtension
                = ExtensionConverter.findExtension(fhir, FhirExtensionUri.PROBLEM_SIGNIFICANCE);

        if (significanceExtension != null) {


            OutputContainer outputContainer = params.getOutputContainer();
            ObservationAdditional observationAdditional = outputContainer.getObservationAdditional();

            CodeableConcept cc = (CodeableConcept)significanceExtension.getValue();
            Coding coding = cc.getCoding().get(0);
            if (coding != null) {
                significanceDisplay = coding.getDisplay();
                String significanceCode = coding.getCode();

                Integer propertyConceptDbid = 12354;
                Integer valueConceptDbid = 54321;
                //we need to look up DBids for both
                try {
                    propertyConceptDbid =
                            IMClient.getConceptDbidForSchemeCode(IMConstant.DISCOVERY_CODE, "CM_ProblemSignificance");
                } catch (Exception e) {

                }

                try {
                    valueConceptDbid =
                            IMClient.getConceptDbidForSchemeCode(IMConstant.SNOMED, significanceCode);
                } catch (Exception e) {

                }
                //transform the IM values to the encounter_additional table upsert
                observationAdditional.writeUpsert(id, propertyConceptDbid, valueConceptDbid, null);
                System.out.println("significance = " + significanceDisplay);
            }
        }
    }

    @Override
    protected SubscriberTableId getMainSubscriberTableId() {
        return SubscriberTableId.OBSERVATION;
    }


}
