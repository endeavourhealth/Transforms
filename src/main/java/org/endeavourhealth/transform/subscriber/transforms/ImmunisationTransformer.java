package org.endeavourhealth.transform.subscriber.transforms;

import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.transform.enterprise.ObservationCodeHelper;
import org.endeavourhealth.transform.subscriber.SubscriberTransformParams;
import org.endeavourhealth.transform.subscriber.outputModels.AbstractSubscriberCsvWriter;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Date;

public class ImmunisationTransformer extends AbstractTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(ImmunisationTransformer.class);

    public boolean shouldAlwaysTransform() {
        return true;
    }

    protected void transformResource(Long enterpriseId,
                                     Resource resource,
                                     AbstractSubscriberCsvWriter csvWriter,
                                     SubscriberTransformParams params) throws Exception {

        Immunization fhir = (Immunization)resource;

        long id;
        long organisationId;
        long patientId;
        long personId;
        Long encounterId = null;
        Long practitionerId = null;
        Date clinicalEffectiveDate = null;
        Integer datePrecisionId = null;
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

        id = enterpriseId.longValue();
        organisationId = params.getEnterpriseOrganisationId().longValue();
        patientId = params.getEnterprisePatientId().longValue();
        personId = params.getEnterprisePersonId().longValue();

        if (fhir.hasEncounter()) {
            Reference encounterReference = fhir.getEncounter();
            encounterId = findEnterpriseId(params, encounterReference);
        }

        if (fhir.hasPerformer()) {
            Reference practitionerReference = fhir.getPerformer();
            practitionerId = transformOnDemandAndMapId(practitionerReference, params);
        }

        if (fhir.hasDateElement()) {
            DateTimeType dt = fhir.getDateElement();
            clinicalEffectiveDate = dt.getValue();
            datePrecisionId = convertDatePrecision(dt.getPrecision());
        }

        ObservationCodeHelper codes = ObservationCodeHelper.extractCodeFields(fhir.getVaccineCode());
        if (codes == null) {
            return;
        }
        snomedConceptId = codes.getSnomedConceptId();
        originalCode = codes.getOriginalCode();
        originalTerm = codes.getOriginalTerm();

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
            parentObservationId = findEnterpriseId(params, parentReference);
        }

        org.endeavourhealth.transform.subscriber.outputModels.Observation model
                = (org.endeavourhealth.transform.subscriber.outputModels.Observation)csvWriter;
        model.writeUpsert(id,
                organisationId,
                patientId,
                personId,
                encounterId,
                practitionerId,
                clinicalEffectiveDate,
                datePrecisionId,
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
                parentObservationId);
    }
}

