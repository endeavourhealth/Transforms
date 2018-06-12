package org.endeavourhealth.transform.enterprise.transforms;

import org.endeavourhealth.common.fhir.*;
import org.endeavourhealth.transform.enterprise.EnterpriseTransformParams;
import org.endeavourhealth.transform.enterprise.outputModels.AbstractEnterpriseCsvWriter;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.crypto.dsig.TransformException;
import java.math.BigDecimal;
import java.util.Date;

public class ObservationTransformer extends AbstractTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(ObservationTransformer.class);

    public boolean shouldAlwaysTransform() {
        return true;
    }

    protected void transformResource(Long enterpriseId,
                          Resource resource,
                          AbstractEnterpriseCsvWriter csvWriter,
                          EnterpriseTransformParams params) throws Exception {

        Observation fhir = (Observation)resource;

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
            for (Reference reference: fhir.getPerformer()) {
                ResourceType resourceType = ReferenceHelper.getResourceType(reference);
                if (resourceType == ResourceType.Practitioner) {
                    practitionerId = transformOnDemandAndMapId(reference, params);
                }
            }
        }

        if (fhir.hasEffectiveDateTimeType()) {
            DateTimeType dt = fhir.getEffectiveDateTimeType();
            clinicalEffectiveDate = dt.getValue();
            datePrecisionId = convertDatePrecision(dt.getPrecision());
        }

        snomedConceptId = CodeableConceptHelper.findSnomedConceptId(fhir.getCode());

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
                CodeableConcept codeableConcept = (CodeableConcept)value;
                resultConcptId = CodeableConceptHelper.findSnomedConceptId(codeableConcept);

            } else {
                throw new TransformException("Unsupported value type " + value.getClass() + " for " + fhir.getResourceType() + " " + fhir.getId());
            }
        }

        //add the raw original code, to assist in data checking
        //prefix the original code with something to identify the scheme
        originalCode = findAndFormatOriginalCode(fhir.getCode());
        //originalCode = CodeableConceptHelper.findOriginalCode(fhir.getCode());

        //add original term too, for easy display of results
        originalTerm = fhir.getCode().getText();

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

        org.endeavourhealth.transform.enterprise.outputModels.Observation model = (org.endeavourhealth.transform.enterprise.outputModels.Observation)csvWriter;
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

    /**
     * we have the original code column to tell us what the non-snomed code was in the source system,
     * but need to unambiguously know what coding scheme that was, so we prefix the original code
     * with a short string to say what it was
     */
    public static String findAndFormatOriginalCode(CodeableConcept codeableConcept) throws Exception {

        Coding originalCoding = CodeableConceptHelper.findOriginalCoding(codeableConcept);
        if (originalCoding == null) {
            return null;
        }

        String system = originalCoding.getSystem();
        if (system.equals(FhirCodeUri.CODE_SYSTEM_READ2)
                || system.equals(FhirCodeUri.CODE_SYSTEM_EMIS_CODE)) {
            //there's already a vast amount of Read2 and Emis data in the table, so it's too late
            //to easily prefix this, so just use the raw code
            return originalCoding.getCode();

        } else if (system.equals(FhirCodeUri.CODE_SYSTEM_SNOMED_CT)
                || system.equals(FhirCodeUri.CODE_SYSTEM_EMISSNOMED)) {

            //a Snomed coding should never be picked up as an "original" term,
            //so something has gone wrong
            throw new TransformException("Original coding has system " + system);

        } else if (system.equals(FhirCodeUri.CODE_SYSTEM_CTV3)) {
            return "CTV3_" + originalCoding.getCode();

        } else if (system.equals(FhirCodeUri.CODE_SYSTEM_ICD10)) {
            return "ICD10_" + originalCoding.getCode();

        } else if (system.equals(FhirCodeUri.CODE_SYSTEM_OPCS4)) {
            return "OPCS4_" + originalCoding.getCode();

        } else if (system.equals(FhirCodeUri.CODE_SYSTEM_CERNER_CODE_ID)) {
            return "CERNER_" + originalCoding.getCode();

        } else {
            throw new TransformException("Unsupported original code system [" + system + "]");
        }
    }
}



