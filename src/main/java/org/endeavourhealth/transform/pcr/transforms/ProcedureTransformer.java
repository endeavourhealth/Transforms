package org.endeavourhealth.transform.pcr.transforms;

import org.endeavourhealth.transform.pcr.PcrTransformParams;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.hl7.fhir.instance.model.Procedure;
import org.hl7.fhir.instance.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcedureTransformer extends AbstractTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(ProcedureTransformer.class);

    public boolean shouldAlwaysTransform() {
        return true;
    }

    protected void transformResource(Long pcrId,
                          Resource resource,
                          AbstractPcrCsvWriter csvWriter,
                          PcrTransformParams params) throws Exception {

        Procedure fhir = (Procedure)resource;

        //TODO - needs Procedure output model (not for v1)

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
//        organisationId = params.getEnterpriseOrganisationId().longValue();
//        patientId = params.getEnterprisePatientId().longValue();
//        personId = params.getEnterprisePersonId().longValue();
//
//        if (fhir.hasEncounter()) {
//            Reference encounterReference = fhir.getEncounter();
//            encounterId = findEnterpriseId(params, encounterReference);
//        }
//
//        if (fhir.hasPerformer()) {
//            if (fhir.getPerformer().size() > 1) {
//                throw new TransformException("Procedures with more than one performer not supported " + fhir.getId());
//            }
//            Procedure.ProcedurePerformerComponent performerComponent = fhir.getPerformer().get(0);
//            Reference practitionerReference = performerComponent.getActor();
//            practitionerId = transformOnDemandAndMapId(practitionerReference, params);
//        }
//
//        if (fhir.hasPerformedDateTimeType()) {
//            DateTimeType dt = fhir.getPerformedDateTimeType();
//            clinicalEffectiveDate = dt.getValue();
//            datePrecisionId = convertDatePrecision(dt.getPrecision());
//        }
//
//        ObservationCodeHelper codes = ObservationCodeHelper.extractCodeFields(fhir.getCode());
//        if (codes == null) {
//            return;
//        }
//        snomedConceptId = codes.getSnomedConceptId();
//        originalCode = codes.getOriginalCode();
//        originalTerm = codes.getOriginalTerm();
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

