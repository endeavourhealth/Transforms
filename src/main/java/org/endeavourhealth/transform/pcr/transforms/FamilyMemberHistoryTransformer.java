package org.endeavourhealth.transform.pcr.transforms;

import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.pcr.PcrTransformParams;
import org.endeavourhealth.transform.pcr.ObservationCodeHelper;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.endeavourhealth.transform.pcr.outputModels.Observation;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Date;

public class FamilyMemberHistoryTransformer extends AbstractTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(FamilyMemberHistoryTransformer.class);

    public boolean shouldAlwaysTransform() {
        return true;
    }

    protected void transformResource(Long enterpriseId,
                          Resource resource,
                          AbstractPcrCsvWriter csvWriter,
                          PcrTransformParams params) throws Exception {

        FamilyMemberHistory fhir = (FamilyMemberHistory)resource;

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

        if (fhir.hasExtension()) {
            for (Extension extension: fhir.getExtension()) {
                if (extension.getUrl().equals(FhirExtensionUri.ASSOCIATED_ENCOUNTER)) {
                    Reference encounterReference = (Reference)extension.getValue();
                    encounterId = findEnterpriseId(params, encounterReference);

                } else if (extension.getUrl().equals(FhirExtensionUri.FAMILY_MEMBER_HISTORY_REPORTED_BY)) {
                    Reference practitionerReference = (Reference)extension.getValue();
                    practitionerId = transformOnDemandAndMapId(practitionerReference, params);
                }
            }
        }

        if (fhir.hasDateElement()) {
            DateTimeType dt = fhir.getDateElement();
            clinicalEffectiveDate = dt.getValue();
            datePrecisionId = convertDatePrecision(dt.getPrecision());
        }

        if (fhir.getCondition().size() > 1) {
            throw new TransformException("FamilyMemberHistory with more than one item not supported");
        }

        //if there's no clinical code, then don't transform - there are a small number of records
        //from Barts where there's no code, so we should skip them on outbound transforms
        if (fhir.getCondition().isEmpty()) {
            return;
        }

        FamilyMemberHistory.FamilyMemberHistoryConditionComponent condition = fhir.getCondition().get(0);

        ObservationCodeHelper codes = ObservationCodeHelper.extractCodeFields(condition.getCode());
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

        Observation model = (Observation)csvWriter;
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

