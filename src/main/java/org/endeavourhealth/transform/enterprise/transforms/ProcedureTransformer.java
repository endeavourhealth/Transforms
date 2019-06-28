package org.endeavourhealth.transform.enterprise.transforms;

import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.reference.CernerClinicalEventMappingDalI;
import org.endeavourhealth.core.database.dal.reference.CernerProcedureMapDalI;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.enterprise.EnterpriseTransformParams;
import org.endeavourhealth.transform.enterprise.ObservationCodeHelper;
import org.endeavourhealth.transform.enterprise.outputModels.AbstractEnterpriseCsvWriter;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Date;

public class ProcedureTransformer extends AbstractTransformer {

    CernerClinicalEventMappingDalI referenceDal = DalProvider.factoryCernerClinicalEventMappingDal();

    private static final Logger LOG = LoggerFactory.getLogger(ProcedureTransformer.class);

    public boolean shouldAlwaysTransform() {
        return true;
    }

    private static CernerProcedureMapDalI cernerProcedureMap = DalProvider.factoryCernerProcedureMapDal();

    protected void transformResource(Long enterpriseId,
                                     Resource resource,
                                     AbstractEnterpriseCsvWriter csvWriter,
                                     EnterpriseTransformParams params) throws Exception {

        Procedure fhir = (Procedure) resource;

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
            if (fhir.getPerformer().size() > 1) {
                throw new TransformException("Procedures with more than one performer not supported " + fhir.getId());
            }
            Procedure.ProcedurePerformerComponent performerComponent = fhir.getPerformer().get(0);
            Reference practitionerReference = performerComponent.getActor();
            practitionerId = transformOnDemandAndMapId(practitionerReference, params);
        }

        if (fhir.hasPerformedDateTimeType()) {
            DateTimeType dt = fhir.getPerformedDateTimeType();
            clinicalEffectiveDate = dt.getValue();
            datePrecisionId = convertDatePrecision(dt.getPrecision());
        } else if (fhir.hasPerformedPeriod()) {
            Period p = fhir.getPerformedPeriod();
            if (p.hasStart()) {
                DateTimeType dt = fhir.getPerformedPeriod().getStartElement();
                clinicalEffectiveDate = dt.getValue();
                datePrecisionId = convertDatePrecision(dt.getPrecision());
            }
        }

        ObservationCodeHelper codes = ObservationCodeHelper.extractCodeFields(fhir.getCode());
        //The above boolean tells the helper to allow some Cerner coded records through. Now filtered below
        if (codes == null) {
            return;
        }
        snomedConceptId = codes.getSnomedConceptId();
        originalCode = codes.getOriginalCode();
        originalTerm = codes.getOriginalTerm();


        /*if (snomedConceptId == null && CodeableConceptHelper.findOriginalCoding(fhir.getCode()) != null) {
            Coding originalCoding = CodeableConceptHelper.findOriginalCoding(fhir.getCode());
            if (originalCoding.getSystem().equalsIgnoreCase(FhirCodeUri.CODE_SYSTEM_CERNER_CODE_ID)) {
                if (StringUtils.isNumeric(originalCoding.getCode())) {
                    Long codeLong = Long.parseLong(originalCoding.getCode());
                    CernerClinicalEventMap mapping = referenceDal.findMappingForCvrefCode(codeLong);
                    if (mapping.getSnomedConceptId() != null) {
                        snomedConceptId = Long.parseLong(mapping.getSnomedConceptId());
                    }

                    //snomedConceptId = cernerProcedureMap.getSnomedFromCernerProc(parseInt(originalCoding.getCode()));
                } else {
                    return; // Don't allow records with only a Cerner term not mapped to Snomed.
                }
            }
        }*/

        Extension reviewExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.IS_REVIEW);
        if (reviewExtension != null) {
            BooleanType b = (BooleanType) reviewExtension.getValue();
            if (b.getValue() != null) {
                isReview = b.getValue();
            }
        }

        Extension parentExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.PARENT_RESOURCE);
        if (parentExtension != null) {
            Reference parentReference = (Reference) parentExtension.getValue();
            parentObservationId = findEnterpriseId(params, parentReference);
        }

        Extension isPrimaryExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.IS_PRIMARY);
        if (isPrimaryExtension != null) {
            BooleanType b = (BooleanType) isPrimaryExtension.getValue();
            if (b.getValue() != null) {
                resultString = "Primary";
            }
        }

        org.endeavourhealth.transform.enterprise.outputModels.Observation model = (org.endeavourhealth.transform.enterprise.outputModels.Observation) csvWriter;

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

