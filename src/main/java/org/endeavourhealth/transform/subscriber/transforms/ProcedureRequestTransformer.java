package org.endeavourhealth.transform.subscriber.transforms;

import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirResourceHelper;
import org.endeavourhealth.im.client.IMClient;
import org.endeavourhealth.transform.subscriber.IMConstant;
import org.endeavourhealth.transform.subscriber.ObservationCodeHelper;
import org.endeavourhealth.transform.subscriber.SubscriberTransformParams;
import org.endeavourhealth.transform.subscriber.targetTables.SubscriberTableId;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class ProcedureRequestTransformer extends AbstractSubscriberTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(ProcedureRequestTransformer.class);

    public boolean shouldAlwaysTransform() {
        return true;
    }

    @Override
    protected void transformResource(SubscriberId subscriberId, ResourceWrapper resourceWrapper, SubscriberTransformParams params) throws Exception {

        org.endeavourhealth.transform.subscriber.targetTables.ProcedureRequest model = params.getOutputContainer().getProcedureRequests();

        if (resourceWrapper.isDeleted()) {
            model.writeDelete(subscriberId);

            //write the event log entry
            writeEventLog(params, resourceWrapper, subscriberId);

            return;
        }

        ProcedureRequest fhir = (ProcedureRequest) FhirResourceHelper.deserialiseResouce(resourceWrapper);

        long organizationId;
        long patientId;
        long personId;
        Long encounterId = null;
        Long practitionerId = null;
        Date clinicalEffectiveDate = null;
        Integer datePrecisionId = null;
        // Long snomedConceptId = null;
        Integer procedureRequestStatusConceptId = null;
        // String originalCode = null;
        // String originalTerm = null;
        Integer coreConceptId = null;
        Integer nonCoreConceptId = null;
        Double ageAtEvent = null;
        Boolean isPrimary = null;

        organizationId = params.getEnterpriseOrganisationId().longValue();
        patientId = params.getEnterprisePatientId().longValue();
        personId = params.getEnterprisePersonId().longValue();

        if (fhir.hasEncounter()) {
            Reference encounterReference = fhir.getEncounter();
            encounterId = findEnterpriseId(params, SubscriberTableId.ENCOUNTER, encounterReference);
        }

        if (fhir.hasOrderer()) {
            Reference practitionerReference = fhir.getOrderer();
            practitionerId = transformOnDemandAndMapId(practitionerReference, params);
        }

        if (fhir.hasScheduledDateTimeType()) {
            DateTimeType dt = fhir.getScheduledDateTimeType();
            clinicalEffectiveDate = dt.getValue();
            datePrecisionId = convertDatePrecision(dt.getPrecision());
        }

        /*
        ObservationCodeHelper codes = ObservationCodeHelper.extractCodeFields(fhir.getCode());
        if (codes == null) {
            return;
        }
        snomedConceptId = codes.getSnomedConceptId();
        originalCode = codes.getOriginalCode();
        originalTerm = codes.getOriginalTerm();
         */
        ObservationCodeHelper codes = ObservationCodeHelper.extractCodeFields(fhir.getCode());
        if (codes == null) {
            return;
        }
        Coding originalCoding = CodeableConceptHelper.findOriginalCoding(fhir.getCode());
        String originalCode = codes.getOriginalCode();
        if (originalCoding == null) {
            originalCoding = fhir.getCode().getCoding().get(0);
            originalCode = fhir.getCode().getCoding().get(0).getCode();
        }

        coreConceptId = IMClient.getMappedCoreConceptIdForSchemeCode(getScheme(originalCoding.getSystem()), originalCode);
        if (coreConceptId == null) {
            LOG.warn("coreConceptId is null using scheme: " + getScheme(originalCoding.getSystem()) + " code: " + originalCode);
            throw new org.endeavourhealth.core.exceptions.TransformException("coreConceptId is null for " + fhir.getResourceType() + " " + fhir.getId());
        }

        nonCoreConceptId = IMClient.getConceptIdForSchemeCode(getScheme(originalCoding.getSystem()), originalCode);
        if (nonCoreConceptId == null) {
            LOG.warn("nonCoreConceptId is null using scheme: " + getScheme(originalCoding.getSystem()) + " code: " + originalCode);
            throw new org.endeavourhealth.core.exceptions.TransformException("nonCoreConceptId is null for " + fhir.getResourceType() + " " + fhir.getId());
        }

        if (fhir.hasStatus()) {
            procedureRequestStatusConceptId = IMClient.getMappedCoreConceptIdForSchemeCode(
                    IMConstant.FHIR_PROCEDURE_REQUEST_STATUS, fhir.getStatus().toCode());
            if (procedureRequestStatusConceptId == null) {
                throw new TransformException("procedureRequestStatusConceptId is null for " + fhir.getResourceType() + " " + fhir.getId());
            }

        }

        if (fhir.getSubjectTarget() != null) {
            Patient patient = (Patient) fhir.getSubjectTarget();
            ageAtEvent = getPatientAgeInMonths(patient);
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
            datePrecisionId,
            procedureRequestStatusConceptId,
            coreConceptId,
            nonCoreConceptId,
            ageAtEvent,
            isPrimary);
    }

    @Override
    protected SubscriberTableId getMainSubscriberTableId() {
        return SubscriberTableId.PROCEDURE_REQUEST;
    }
}
