package org.endeavourhealth.transform.subscriber.transforms;

import com.google.common.base.Strings;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.im.client.IMClient;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.enterprise.ObservationCodeHelper;
import org.endeavourhealth.transform.subscriber.IMConstant;
import org.endeavourhealth.transform.subscriber.IMHelper;
import org.endeavourhealth.transform.subscriber.SubscriberTransformHelper;
import org.endeavourhealth.transform.subscriber.targetTables.ObservationAdditional;
import org.endeavourhealth.transform.subscriber.targetTables.OutputContainer;
import org.endeavourhealth.transform.subscriber.targetTables.SubscriberTableId;
import org.hl7.fhir.instance.model.*;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.crypto.dsig.TransformException;
import java.math.BigDecimal;
import java.util.Date;

public class ObservationTransformer extends AbstractSubscriberTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(ObservationTransformer.class);

    @Override
    protected ResourceType getExpectedResourceType() {
        return ResourceType.Observation;
    }

    public boolean shouldAlwaysTransform() {
        return true;
    }

    @Override
    protected void transformResource(SubscriberId subscriberId, ResourceWrapper resourceWrapper, SubscriberTransformHelper params) throws Exception {

        org.endeavourhealth.transform.subscriber.targetTables.Observation model = params.getOutputContainer().getObservations();

        Observation fhir = (Observation)resourceWrapper.getResource(); //returns null if deleted

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

        if (fhir.hasPerformer()) {
            for (Reference reference: fhir.getPerformer()) {
                ResourceType resourceType = ReferenceHelper.getResourceType(reference);
                if (resourceType == ResourceType.Practitioner) {
                    practitionerId = transformOnDemandAndMapId(reference, SubscriberTableId.PRACTITIONER, params);
                }
            }
        }

        if (fhir.hasEffectiveDateTimeType()) {
            DateTimeType dt = fhir.getEffectiveDateTimeType();
            clinicalEffectiveDate = dt.getValue();
            datePrecisionConceptId = convertDatePrecision(params, fhir, dt.getPrecision(), clinicalEffectiveDate.toString());
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

        if (fhir.hasValue()) {
            Type value = fhir.getValue();
            if (value instanceof Quantity) {
                Quantity quantity = (Quantity)value;
                resultValue = quantity.getValue();
                resultValueUnits = quantity.getUnit();

            } else if (value instanceof DateTimeType) {
                DateTimeType dateTimeType = (DateTimeType)value;
                resultDate = dateTimeType.getValue();

            } else if (value instanceof StringType) {
                StringType stringType = (StringType)value;
                resultString = stringType.getValue();

            } else if (value instanceof CodeableConcept) {
                CodeableConcept resultCodeableConcept = (CodeableConcept)value;
                resultConceptId = CodeableConceptHelper.findSnomedConceptId(resultCodeableConcept);

            } else {
                throw new TransformException("Unsupported value type " + value.getClass() + " for " + fhir.getResourceType() + " " + fhir.getId());
            }
        }

        Extension reviewExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.IS_REVIEW);
        if (reviewExtension != null) {
            BooleanType b = (BooleanType)reviewExtension.getValue();
            if (b.getValue() != null) {
                isReview = b.getValue().booleanValue();
            }
        }

        parentObservationId = transformParentResourceReference(fhir, params);

        if (fhir.getSubject() != null) {
            Reference ref = fhir.getSubject();
            Patient patient = params.getCachedPatient(ref);
            ageAtEvent = getPatientAgeInDecimalYears(patient, clinicalEffectiveDate);
        }

        Extension isPrimaryExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.IS_PRIMARY);
        if (isPrimaryExtension != null) {
            BooleanType b = (BooleanType)isPrimaryExtension.getValue();
            if (b.getValue() != null) {
                isPrimary = b.getValue();
            }
        }

        Extension episodicityExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.PROBLEM_EPISODICITY);
        if (episodicityExtension != null) {
            StringType episodicity = (StringType) episodicityExtension.getValue();
            episodicityConceptId = IMHelper.getIMConcept(params, fhir, IMConstant.FHIR_CONDITION_EPISODICITY,
                    episodicity.getValue(), episodicity.getValue());
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
        // transformAdditionals(fhir, params, subscriberId);

    }

    public static Long transformParentResourceReference(DomainResource fhir, SubscriberTransformHelper params) throws Exception {

        Extension parentExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.PARENT_RESOURCE);
        if (parentExtension == null) {
            return null;
        }

        Reference parentReference = (Reference)parentExtension.getValue();
        ResourceType parentType = ReferenceHelper.getResourceType(parentReference);
        if (parentType == ResourceType.DiagnosticOrder) {
            return transformOnDemandAndMapId(parentReference, SubscriberTableId.DIAGNOSTIC_ORDER, params);

        } else if (parentType == ResourceType.Observation
                || parentType == ResourceType.Condition
                || parentType == ResourceType.Procedure
                || parentType == ResourceType.FamilyMemberHistory
                || parentType == ResourceType.Immunization
                || parentType == ResourceType.DiagnosticReport
                || parentType == ResourceType.Specimen) {
            return transformOnDemandAndMapId(parentReference, SubscriberTableId.OBSERVATION, params);

        } else if (parentType == ResourceType.AllergyIntolerance) {
            return transformOnDemandAndMapId(parentReference, SubscriberTableId.ALLERGY_INTOLERANCE, params);

        } else if (parentType == ResourceType.ReferralRequest) {
            return transformOnDemandAndMapId(parentReference, SubscriberTableId.REFERRAL_REQUEST, params);

        } else {
            //if it's one of these resource types, then the table doesn't support the link, so ignore
            throw new Exception("Unexpected parent resource type " + parentType + " for " + fhir.getResourceType() + " " + fhir.getId());
        }
    }

    private void transformAdditionals(Resource resource, SubscriberTransformHelper params, SubscriberId id) throws Exception {

        Observation fhir = (Observation)resource;

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
                System.out.println("observation significance : " + significanceDisplay);
            }
        }

        //if it has no extension data, then nothing further to do
        if (!fhir.hasReferenceRange()) {
            return;
        }
        String fhirJson = FhirSerializationHelper.serializeResource(fhir);

        com.google.gson.JsonParser parser = new com.google.gson.JsonParser();

        JsonElement jsonElement = parser.parse(fhirJson);

        JsonObject jsonObject = jsonElement.getAsJsonObject();

        String reference = jsonObject.get("referenceRange").toString();

        System.out.println("new ref :" + reference);

        if (!Strings.isNullOrEmpty(reference)) {

            OutputContainer outputContainer = params.getOutputContainer();
            ObservationAdditional observationAdditional = outputContainer.getObservationAdditional();

            Integer propertyConceptDbid = 45678;
            //we need to look up DBids property

            try {
                propertyConceptDbid =
                        IMClient.getConceptDbidForSchemeCode(IMConstant.DISCOVERY_CODE, "CM_ResultReferenceRange");
            } catch (Exception e) {

            }

            //transform the IM values to the encounter_additional table upsert
            observationAdditional.writeUpsert(id, propertyConceptDbid,null,  reference);
            System.out.println("refRange : " + reference);
        }
    }

    private void transformPatientDelays(Resource resource, SubscriberTransformHelper params, SubscriberId id) throws Exception {

        Observation fhir = (Observation)resource;

        DateTimeType delayDaysStringType
                = (DateTimeType)ExtensionConverter.findExtensionValue(fhir, FhirExtensionUri.OBSERVATION_PATIENT_DELAY_DAYS);

        if (delayDaysStringType != null) {

            String delayDays = delayDaysStringType.getValue().toString();
            OutputContainer outputContainer = params.getOutputContainer();
            ObservationAdditional observationAdditional = outputContainer.getObservationAdditional();

            Integer propertyConceptDbid = 0;
            //TODO : Need to change
            Integer valueConceptDbid = 54321;

                propertyConceptDbid =
                        IMClient.getConceptDbidForSchemeCode(IMConstant.DISCOVERY_CODE, "CM_PatientDelayDays");

            String jsonString = new JSONObject()
                    .put("date_value", delayDays).toString();
            observationAdditional.writeUpsert(id, propertyConceptDbid,null,  jsonString);

        }
    }

    @Override
    protected SubscriberTableId getMainSubscriberTableId() {
        return SubscriberTableId.OBSERVATION;
    }

}
