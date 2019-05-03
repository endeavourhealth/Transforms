package org.endeavourhealth.transform.subscriber.transforms;

import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.im.client.IMClient;
import org.endeavourhealth.transform.subscriber.ObservationCodeHelper;
import org.endeavourhealth.transform.subscriber.SubscriberTransformParams;
import org.endeavourhealth.transform.subscriber.outputModels.AbstractSubscriberCsvWriter;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class AllergyIntoleranceTransformer extends AbstractTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(AllergyIntoleranceTransformer.class);

    public boolean shouldAlwaysTransform() {
        return true;
    }

    protected void transformResource(Long enterpriseId,
                          Resource resource,
                          AbstractSubscriberCsvWriter csvWriter,
                                     SubscriberTransformParams params) throws Exception {

        AllergyIntolerance fhir = (AllergyIntolerance)resource;

        long id;
        long organizationId;
        long patientId;
        long personId;
        Long encounterId = null;
        Long practitionerId = null;
        Date clinicalEffectiveDate = null;
        Integer datePrecisionId = null;
        // Long snomedConceptId = null;
        // String originalCode = null;
        // String originalTerm = null;
        boolean isReview = false;
        Integer coreConceptId = null;
        Integer nonCoreConceptId = null;
        Double ageAtEvent = null;
        Boolean isPrimary = null;

        id = enterpriseId.longValue();
        organizationId = params.getEnterpriseOrganisationId().longValue();
        patientId = params.getEnterprisePatientId().longValue();
        personId = params.getEnterprisePersonId().longValue();

        if (fhir.hasExtension()) {
            for (Extension extension: fhir.getExtension()) {
                if (extension.getUrl().equals(FhirExtensionUri.ASSOCIATED_ENCOUNTER)) {
                    Reference encounterReference = (Reference)extension.getValue();
                    encounterId = findEnterpriseId(params, encounterReference);
                }
            }
        }

        //note that the "recorder" field is actually used to store the named clinician,
        //and the standard "recorded by" extension is used to store who physically entered it into the source software
        if (fhir.hasRecorder()) {
            Reference practitionerReference = fhir.getRecorder();
            practitionerId = transformOnDemandAndMapId(practitionerReference, params);
        }

        if (fhir.hasOnset()) {
            DateTimeType dt = fhir.getOnsetElement();
            clinicalEffectiveDate = dt.getValue();
            datePrecisionId = convertDatePrecision(dt.getPrecision());

        }

        /* ObservationCodeHelper codes = ObservationCodeHelper.extractCodeFields(fhir.getSubstance());
        if (codes == null) {
            return;
        }
        snomedConceptId = codes.getSnomedConceptId();
        originalCode = codes.getOriginalCode();
        originalTerm = codes.getOriginalTerm();*/

        Extension reviewExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.IS_REVIEW);
        if (reviewExtension != null) {
            BooleanType b = (BooleanType)reviewExtension.getValue();
            if (b.getValue() != null) {
                isReview = b.getValue();
            }
        }

        ObservationCodeHelper codes = ObservationCodeHelper.extractCodeFields(fhir.getSubstance());
        if (codes == null) {
            return;
        }
        Coding originalCoding = CodeableConceptHelper.findOriginalCoding(fhir.getSubstance());
        String originalCode = codes.getOriginalCode();
        if (originalCoding == null) {
            originalCoding = fhir.getSubstance().getCoding().get(0);
            originalCode = fhir.getSubstance().getCoding().get(0).getCode();
        }

        coreConceptId = IMClient.getMappedCoreConceptIdForSchemeCode(getScheme(originalCoding.getSystem()), originalCode);
        if (coreConceptId == null) {
            LOG.warn("coreConceptId is null using scheme: " + getScheme(originalCoding.getSystem()) + " code: " + originalCode);
            throw new TransformException("coreConceptId is null for " + fhir.getResourceType() + " " + fhir.getId());
        }

        nonCoreConceptId = IMClient.getConceptIdForSchemeCode(getScheme(originalCoding.getSystem()), originalCode);
        if (nonCoreConceptId == null) {
            LOG.warn("nonCoreConceptId is null using scheme: " + getScheme(originalCoding.getSystem()) + " code: " + originalCode);
            throw new TransformException("nonCoreConceptId is null for " + fhir.getResourceType() + " " + fhir.getId());
        }

        if (fhir.getPatientTarget() != null) {
            ageAtEvent = getPatientAgeInMonths(fhir.getPatientTarget());
        }

        Extension isPrimaryExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.IS_PRIMARY);
        if (isPrimaryExtension != null) {
            BooleanType b = (BooleanType)isPrimaryExtension.getValue();
            if (b.getValue() != null) {
                isPrimary = b.getValue();
            }
        }

        org.endeavourhealth.transform.subscriber.outputModels.AllergyIntolerance model
                = (org.endeavourhealth.transform.subscriber.outputModels.AllergyIntolerance)csvWriter;
        model.writeUpsert(id,
            organizationId,
            patientId,
            personId,
            encounterId,
            practitionerId,
            clinicalEffectiveDate,
            datePrecisionId,
            isReview,
            coreConceptId,
            nonCoreConceptId,
            ageAtEvent,
            isPrimary);
    }

}
