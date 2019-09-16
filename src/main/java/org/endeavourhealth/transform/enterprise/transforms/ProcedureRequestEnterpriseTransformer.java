package org.endeavourhealth.transform.enterprise.transforms;

import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.enterprise.EnterpriseTransformHelper;
import org.endeavourhealth.transform.enterprise.ObservationCodeHelper;
import org.endeavourhealth.transform.enterprise.outputModels.AbstractEnterpriseCsvWriter;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class ProcedureRequestEnterpriseTransformer extends AbstractEnterpriseTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(ProcedureRequestEnterpriseTransformer.class);

    @Override
    protected ResourceType getExpectedResourceType() {
        return ResourceType.ProcedureRequest;
    }

    public boolean shouldAlwaysTransform() {
        return true;
    }

    protected void transformResource(Long enterpriseId,
                                     ResourceWrapper resourceWrapper,
                                     AbstractEnterpriseCsvWriter csvWriter,
                                     EnterpriseTransformHelper params) throws Exception {

        ProcedureRequest fhir = (ProcedureRequest)resourceWrapper.getResource(); //returns null if deleted

        //if deleted, confidential or the entire patient record shouldn't be there, then delete
        if (resourceWrapper.isDeleted()
                || isConfidential(fhir)
                || params.getShouldPatientRecordBeDeleted()) {
            csvWriter.writeDelete(enterpriseId.longValue());
            return;
        }

        long id;
        long organisationId;
        long patientId;
        long personId;
        Long encounterId = null;
        Long practitionerId = null;
        Date clinicalEffectiveDate = null;
        Integer datePrecisionId = null;
        Long snomedConceptId = null;
        Integer procedureRequestStatusId = null;
        String originalCode = null;
        String originalTerm = null;

        id = enterpriseId.longValue();
        organisationId = params.getEnterpriseOrganisationId().longValue();
        patientId = params.getEnterprisePatientId().longValue();
        personId = params.getEnterprisePersonId().longValue();

        if (fhir.hasEncounter()) {
            Reference encounterReference = fhir.getEncounter();
            encounterId = transformOnDemandAndMapId(encounterReference, params);
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

        ObservationCodeHelper codes = ObservationCodeHelper.extractCodeFields(fhir.getCode());
        if (codes == null) {
            return;
        }
        snomedConceptId = codes.getSnomedConceptId();
        originalCode = codes.getOriginalCode();
        originalTerm = codes.getOriginalTerm();

        if (fhir.hasStatus()) {
            procedureRequestStatusId = new Integer(fhir.getStatus().ordinal());
        }

        org.endeavourhealth.transform.enterprise.outputModels.ProcedureRequest model = (org.endeavourhealth.transform.enterprise.outputModels.ProcedureRequest)csvWriter;
        model.writeUpsert(id,
            organisationId,
            patientId,
            personId,
            encounterId,
            practitionerId,
            clinicalEffectiveDate,
            datePrecisionId,
            snomedConceptId,
            procedureRequestStatusId,
            originalCode,
            originalTerm);
    }
}

