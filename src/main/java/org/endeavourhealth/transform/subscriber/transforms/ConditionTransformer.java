package org.endeavourhealth.transform.subscriber.transforms;

import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.core.fhirStorage.FhirResourceHelper;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.enterprise.ObservationCodeHelper;
import org.endeavourhealth.transform.subscriber.IMConstant;
import org.endeavourhealth.transform.subscriber.IMHelper;
import org.endeavourhealth.transform.subscriber.SubscriberTransformHelper;
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

        Condition fhir = (Condition)resourceWrapper.getResource(); //returns null if deleted

        //if deleted, confidential or the entire patient record shouldn't be there, then delete
        if (resourceWrapper.isDeleted()
                //|| isConfidential(fhir)
                || params.getShouldPatientRecordBeDeleted()
                || params.shouldClinicalConceptBeDeleted(fhir.getCode())) {
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

        String conceptScheme = getScheme(originalCoding.getSystem());
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


    }

    @Override
    protected SubscriberTableId getMainSubscriberTableId() {
        return SubscriberTableId.OBSERVATION;
    }


}
