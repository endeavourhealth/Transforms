package org.endeavourhealth.transform.subscriber.transforms;

import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.core.exceptions.TransformException;
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

public class DiagnosticOrderTransformer extends AbstractSubscriberTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(DiagnosticOrderTransformer.class);

    @Override
    protected ResourceType getExpectedResourceType() {
        return ResourceType.DiagnosticOrder;
    }

    public boolean shouldAlwaysTransform() {
        return true;
    }

    @Override
    protected void transformResource(SubscriberId subscriberId, ResourceWrapper resourceWrapper, SubscriberTransformHelper params) throws Exception {

        org.endeavourhealth.transform.subscriber.targetTables.DiagnosticOrder model = params.getOutputContainer().getDiagnosticOrder();

        DiagnosticOrder fhir = (DiagnosticOrder)resourceWrapper.getResource(); //returns null if deleted

        //if deleted, confidential or the entire patient record shouldn't be there, then delete
        if (resourceWrapper.isDeleted()
                //|| isConfidential(fhir)
                || params.getShouldPatientRecordBeDeleted()) {
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

        organizationId = params.getSubscriberOrganisationId().longValue();
        patientId = params.getSubscriberPatientId().longValue();
        personId = params.getSubscriberPersonId().longValue();

        if (fhir.hasEncounter()) {
            Reference encounterReference = fhir.getEncounter();
            encounterId = transformOnDemandAndMapId(encounterReference, SubscriberTableId.ENCOUNTER, params);
        }

        if (fhir.hasOrderer()) {
            Reference practitionerReference = fhir.getOrderer();
            practitionerId = transformOnDemandAndMapId(practitionerReference, SubscriberTableId.PRACTITIONER, params);
        }

        if (fhir.hasEvent()) {
            DiagnosticOrder.DiagnosticOrderEventComponent event = fhir.getEvent().get(0);
            if (event.hasDateTimeElement()) {
                DateTimeType dt = event.getDateTimeElement();
                clinicalEffectiveDate = dt.getValue();
                datePrecisionConceptId = convertDatePrecision(params, fhir, dt.getPrecision(), clinicalEffectiveDate.toString());
            }
        }

        if (fhir.getItem().size() > 1) {
            throw new TransformException("DiagnosticOrder with more than one item not supported");
        }
        DiagnosticOrder.DiagnosticOrderItemComponent item = fhir.getItem().get(0);

        Coding originalCoding = ObservationCodeHelper.findOriginalCoding(item.getCode());
        if (originalCoding == null) {
            //SD-297 - no longer needed
            //TransformWarnings.log(LOG, params, "No suitable Coding found for {} {}", fhir.getResourceType(), fhir.getId());
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

        Extension reviewExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.IS_REVIEW);
        if (reviewExtension != null) {
            BooleanType b = (BooleanType)reviewExtension.getValue();
            if (b.getValue() != null) {
                isReview = b.getValue();
            }
        }

        parentObservationId = ObservationTransformer.transformParentResourceReference(fhir, params);

        if (fhir.getSubject() != null) {
            Reference ref = fhir.getSubject();
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
                isPrimary);

    }

    @Override
    protected SubscriberTableId getMainSubscriberTableId() {
        return SubscriberTableId.DIAGNOSTIC_ORDER;
    }

}
