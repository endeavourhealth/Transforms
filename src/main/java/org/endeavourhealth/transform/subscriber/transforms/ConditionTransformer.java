package org.endeavourhealth.transform.subscriber.transforms;

import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.core.fhirStorage.FhirResourceHelper;
import org.endeavourhealth.transform.pcr.FhirToPcrCsvTransformer;
import org.endeavourhealth.transform.subscriber.ObservationCodeHelper;
import org.endeavourhealth.transform.subscriber.SubscriberTransformParams;
import org.endeavourhealth.transform.subscriber.targetTables.SubscriberTableId;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Date;

public class ConditionTransformer extends AbstractSubscriberTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(ConditionTransformer.class);

    public boolean shouldAlwaysTransform() {
        return true;
    }

    @Override
    protected void transformResource(SubscriberId subscriberId, ResourceWrapper resourceWrapper, SubscriberTransformParams params) throws Exception {

        org.endeavourhealth.transform.subscriber.targetTables.Observation model = params.getOutputContainer().getObservations();

        if (resourceWrapper.isDeleted()) {

            model.writeDelete(subscriberId);

            return;
        }


        Condition fhir = (Condition) FhirResourceHelper.deserialiseResouce(resourceWrapper);

        long id;
        long organisationId;
        long patientId;
        long personId;
        Long encounterId = null;
        Long practitionerId = null;
        Date clinicalEffectiveDate = null;
        Integer datePrecisionConceptId = null;
        Long snomedConceptId = null;
        BigDecimal resultValue = null;
        String resultValueUnits = null;
        Date resultDate = null;
        String resultString = null;
        Long resultConcptId = null;
        String originalCode = null;
        boolean isProblem = false;
        String originalTerm = null;
        boolean isReview = false;
        Date problemEndDate = null;
        Long parentObservationId = null;
        Double ageAtEvent = null;
        Long episodicityConceptId = FhirToPcrCsvTransformer.IM_PLACE_HOLDER;
        Boolean isPrimary = null;

        id = subscriberId.getSubscriberId();
        organisationId = params.getEnterpriseOrganisationId().longValue();
        patientId = params.getEnterprisePatientId().longValue();
        personId = params.getEnterprisePersonId().longValue();

        if (fhir.hasEncounter()) {
            Reference encounterReference = fhir.getEncounter();
            encounterId = findEnterpriseId(params, SubscriberTableId.ENCOUNTER, encounterReference);
        }

        if (fhir.hasAsserter()) {
            Reference practitionerReference = fhir.getAsserter();
            practitionerId = transformOnDemandAndMapId(practitionerReference, params);
        }

        if (fhir.hasOnsetDateTimeType()) {
            DateTimeType dt = fhir.getOnsetDateTimeType();
            clinicalEffectiveDate = dt.getValue();
            datePrecisionConceptId = convertDatePrecision(dt.getPrecision());
        }

        ObservationCodeHelper codes = ObservationCodeHelper.extractCodeFields(fhir.getCode());
        if (codes == null) {
            return;
        }
        snomedConceptId = codes.getSnomedConceptId();
        originalCode = codes.getOriginalCode();
        originalTerm = codes.getOriginalTerm();

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

        Extension parentExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.PARENT_RESOURCE);
        if (parentExtension != null) {
            Reference parentReference = (Reference)parentExtension.getValue();
            parentObservationId = findEnterpriseId(params, SubscriberTableId.OBSERVATION, parentReference);
        }

        if (fhir.getPatientTarget() != null) {
            ageAtEvent = getPatientAgeInMonths(fhir.getPatientTarget());
        }

        Extension episodicityExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.PROBLEM_EPISODICITY);
        if (episodicityExtension != null) {

            StringType episodicityType = (StringType) episodicityExtension.getValue();
            // episodicityConceptId = FhirToPcrCsvTransformer.IM_PLACE_HOLDER;
            episodicityConceptId  = FhirToPcrCsvTransformer.IM_PLACE_HOLDER;
            //IMClient.getConceptId("FhirExtensionUri.PROBLEM_EPISODICITY");
            //TODO do we know how extension uri is mapped?
        }

        Extension isPrimaryExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.IS_PRIMARY);
        if (isPrimaryExtension != null) {
            BooleanType b = (BooleanType)isPrimaryExtension.getValue();
            if (b.getValue() != null) {
                isPrimary = b.getValue();
            }
        }

        //TODO - finish
        /*model.writeUpsert(subscriberId,
                organisationId,
                patientId,
                personId,
                encounterId,
                practitionerId,
                clinicalEffectiveDate,
                datePrecisionConceptId,
                snomedConceptId,
                resultValue,
                resultValueUnits,
                resultDate,
                resultString,
                resultConcptId,
                originalCode,
                isProblem,
                originalTerm,
                isReview,
                problemEndDate,
                parentObservationId,
                ageAtEvent,
                episodicityConceptId,
                isPrimary);
         */

    }

    @Override
    protected SubscriberTableId getMainSubscriberTableId() {
        return SubscriberTableId.OBSERVATION;
    }


}
