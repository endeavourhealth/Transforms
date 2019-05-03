package org.endeavourhealth.transform.subscriber.transforms;

import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.im.client.IMClient;
import org.endeavourhealth.transform.subscriber.IMConstant;
import org.endeavourhealth.transform.subscriber.ObservationCodeHelper;
import org.endeavourhealth.transform.subscriber.SubscriberTransformParams;
import org.endeavourhealth.transform.subscriber.outputModels.AbstractSubscriberCsvWriter;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class ProcedureRequestTransformer extends AbstractTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(ProcedureRequestTransformer.class);

    public boolean shouldAlwaysTransform() {
        return true;
    }

    protected void transformResource(Long enterpriseId,
                                     Resource resource,
                                     AbstractSubscriberCsvWriter csvWriter,
                                     SubscriberTransformParams params) throws Exception {

        ProcedureRequest fhir = (ProcedureRequest)resource;

        long id;
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

        id = enterpriseId.longValue();
        organizationId = params.getEnterpriseOrganisationId().longValue();
        patientId = params.getEnterprisePatientId().longValue();
        personId = params.getEnterprisePersonId().longValue();

        if (fhir.hasEncounter()) {
            Reference encounterReference = fhir.getEncounter();
            encounterId = findEnterpriseId(params, encounterReference);
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

        org.endeavourhealth.transform.subscriber.outputModels.ProcedureRequest model
                = (org.endeavourhealth.transform.subscriber.outputModels.ProcedureRequest)csvWriter;
        model.writeUpsert(id,
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
}
