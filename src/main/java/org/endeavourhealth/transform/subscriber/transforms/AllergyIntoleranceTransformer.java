package org.endeavourhealth.transform.subscriber.transforms;

import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.transform.enterprise.ObservationCodeHelper;
import org.endeavourhealth.transform.subscriber.IMHelper;
import org.endeavourhealth.transform.subscriber.SubscriberTransformHelper;
import org.endeavourhealth.transform.subscriber.targetTables.SubscriberTableId;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class AllergyIntoleranceTransformer extends AbstractSubscriberTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(AllergyIntoleranceTransformer.class);

    @Override
    protected ResourceType getExpectedResourceType() {
        return ResourceType.AllergyIntolerance;
    }

    public boolean shouldAlwaysTransform() {
        return true;
    }

    @Override
    protected void transformResource(SubscriberId subscriberId, ResourceWrapper resourceWrapper, SubscriberTransformHelper params) throws Exception {

        org.endeavourhealth.transform.subscriber.targetTables.AllergyIntolerance model = params.getOutputContainer().getAllergyIntolerances();

        AllergyIntolerance fhir = (AllergyIntolerance)resourceWrapper.getResource(); //returns null if deleted

        //if deleted, confidential or the entire patient record shouldn't be there, then delete
        if (resourceWrapper.isDeleted()
                //|| isConfidential(fhir)
                || params.getShouldPatientRecordBeDeleted()
                || isEnded(fhir)) {
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
        boolean isReview = false;
        Integer coreConceptId = null;
        Integer nonCoreConceptId = null;
        Double ageAtEvent = null;
        Date dateRecorded = null;

        organizationId = params.getSubscriberOrganisationId().longValue();
        patientId = params.getSubscriberPatientId().longValue();
        personId = params.getSubscriberPersonId().longValue();

        if (fhir.hasExtension()) {
            for (Extension extension: fhir.getExtension()) {
                if (extension.getUrl().equals(FhirExtensionUri.ASSOCIATED_ENCOUNTER)) {
                    Reference encounterReference = (Reference)extension.getValue();
                    encounterId = transformOnDemandAndMapId(encounterReference, SubscriberTableId.ENCOUNTER, params);
                }
            }
        }

        //note that the "recorder" field is actually used to store the named clinician,
        //and the standard "recorded by" extension is used to store who physically entered it into the source software
        if (fhir.hasRecorder()) {
            Reference practitionerReference = fhir.getRecorder();
            practitionerId = transformOnDemandAndMapId(practitionerReference, SubscriberTableId.PRACTITIONER, params);
        }

        if (fhir.hasOnset()) {
            DateTimeType dt = fhir.getOnsetElement();
            clinicalEffectiveDate = dt.getValue();
            datePrecisionConceptId = convertDatePrecision(params, fhir, dt.getPrecision(), clinicalEffectiveDate.toString());

        }

        Extension reviewExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.IS_REVIEW);
        if (reviewExtension != null) {
            BooleanType b = (BooleanType)reviewExtension.getValue();
            if (b.getValue() != null) {
                isReview = b.getValue();
            }
        }

        Coding originalCoding = ObservationCodeHelper.findOriginalCoding(fhir.getSubstance());
        if (originalCoding == null) {
            //SD-297 - no longer needed
            //TransformWarnings.log(LOG, params, "No suitable Coding found for {} {}", fhir.getResourceType(), fhir.getId());
            return;
        }
        String originalCode = originalCoding.getCode();
        String conceptScheme = ObservationCodeHelper.mapCodingSystemToImScheme(originalCoding);
        String restrictVSet = params.getConfig().getImRestrictedValueSet();
        nonCoreConceptId = IMHelper.getIMConcept(params, fhir, conceptScheme, originalCode, originalCoding.getDisplay(), restrictVSet);
        if (nonCoreConceptId == null) {
            //if no concept returned (filtered out via value set), then skip the resource
            return;
        }
        coreConceptId = IMHelper.getIMMappedConcept(params, fhir, conceptScheme, originalCode);

        if (fhir.getPatient() != null) {
            Reference ref = fhir.getPatient();
            Patient patient = params.getCachedPatient(ref);
            ageAtEvent = getPatientAgeInDecimalYears(patient, clinicalEffectiveDate);
        }

        dateRecorded = params.includeDateRecorded(fhir);

        model.setIncludeDateRecorded(params.isIncludeDateRecorded());
        model.writeUpsert(
                subscriberId,
                organizationId,
                patientId,
                personId,
                encounterId,
                practitionerId,
                clinicalEffectiveDate,
                datePrecisionConceptId,
                isReview,
                coreConceptId,
                nonCoreConceptId,
                ageAtEvent,
                dateRecorded,
                params);
    }

    /**
     * SD-489 - if the allergy has an explicit end date or is set to be inactive then we want it excluded
     */
    public static boolean isEnded(AllergyIntolerance fhir) {

        if (fhir.hasStatus()
                && (fhir.getStatus() == AllergyIntolerance.AllergyIntoleranceStatus.INACTIVE
                || fhir.getStatus() == AllergyIntolerance.AllergyIntoleranceStatus.RESOLVED
                || fhir.getStatus() == AllergyIntolerance.AllergyIntoleranceStatus.REFUTED
                || fhir.getStatus() == AllergyIntolerance.AllergyIntoleranceStatus.ENTEREDINERROR)) {
            return true;
        }

        if (fhir.hasLastOccurence()) {
            return true;
        }

        return false;
    }

    @Override
    protected SubscriberTableId getMainSubscriberTableId() {
        return SubscriberTableId.ALLERGY_INTOLERANCE;
    }


}
