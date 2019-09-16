package org.endeavourhealth.transform.pcr.transforms;

import org.endeavourhealth.transform.pcr.PcrTransformParams;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.Specimen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpecimenTransformer extends AbstractTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(SpecimenTransformer.class);

    public boolean shouldAlwaysTransform() {
        return true;
    }

    protected void transformResource(Long pcrId,
                          Resource resource,
                          AbstractPcrCsvWriter csvWriter,
                          PcrTransformParams params) throws Exception {

        Specimen fhir = (Specimen)resource;

        //TODO - needs output model to map to in PCR (not for v1)

//        long id;
//        long organisationId;
//        long patientId;
//        long personId;
//        Long encounterId = null;
//        Long practitionerId = null;
//        Date clinicalEffectiveDate = null;
//        Integer datePrecisionId = null;
//        Long snomedConceptId = null;
//        BigDecimal resultValue = null;
//        String resultValueUnits = null;
//        Date resultDate = null;
//        String resultString = null;
//        Long resultConcptId = null;
//        String originalCode = null;
//        boolean isProblem = false;
//        String originalTerm = null;
//        boolean isReview = false;
//        Date problemEndDate = null;
//        Long parentObservationId = null;
//
//        id = enterpriseId.longValue();
//        organisationId = params.getSubscriberOrganisationId().longValue();
//        patientId = params.getSubscriberPatientId().longValue();
//        personId = params.getSubscriberPersonId().longValue();
//
//        if (fhir.hasExtension()) {
//            for (Extension extension: fhir.getExtension()) {
//                if (extension.getUrl().equals(FhirExtensionUri.ASSOCIATED_ENCOUNTER)) {
//                    Reference encounterReference = (Reference)extension.getValue();
//                    encounterId = findEnterpriseId(params, encounterReference);
//                }
//            }
//        }
//
//        if (fhir.hasCollection()) {
//
//            Specimen.SpecimenCollectionComponent fhirCollection = fhir.getCollection();
//
//            if (fhirCollection.hasCollectedDateTimeType()) {
//                DateTimeType dt = fhirCollection.getCollectedDateTimeType();
//                clinicalEffectiveDate = dt.getValue();
//                datePrecisionId = convertDatePrecision(dt.getPrecision());
//            }
//
//            if (fhirCollection.hasCollector()) {
//                Reference practitionerReference = fhirCollection.getCollector();
//                practitionerId = transformOnDemandAndMapId(practitionerReference, params);
//            }
//        }
//
//        ObservationCodeHelper codes = ObservationCodeHelper.extractCodeFields(fhir.getType());
//        if (codes == null) {
//            return;
//        }
//        snomedConceptId = codes.getSnomedConceptId();
//        originalCode = codes.getOriginalCode();
//        originalTerm = codes.getOriginalTerm();
//
//
//        Extension reviewExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.IS_REVIEW);
//        if (reviewExtension != null) {
//            BooleanType b = (BooleanType)reviewExtension.getValue();
//            if (b.getValue() != null) {
//                isReview = b.getValue();
//            }
//        }
//
//        Extension parentExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.PARENT_RESOURCE);
//        if (parentExtension != null) {
//            Reference parentReference = (Reference)parentExtension.getValue();
//            parentObservationId = findEnterpriseId(params, parentReference);
//        }
//
//        Observation model = (Observation)csvWriter;
//        model.writeUpsert(id,
//                organisationId,
//                patientId,
//                personId,
//                encounterId,
//                practitionerId,
//                clinicalEffectiveDate,
//                datePrecisionId,
//                snomedConceptId,
//                resultValue,
//                resultValueUnits,
//                resultDate,
//                resultString,
//                resultConcptId,
//                originalCode,
//                isProblem,
//                originalTerm,
//                isReview,
//                problemEndDate,
//                parentObservationId);
    }
}


