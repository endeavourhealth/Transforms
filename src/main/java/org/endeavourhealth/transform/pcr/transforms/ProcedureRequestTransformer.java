package org.endeavourhealth.transform.pcr.transforms;

import org.endeavourhealth.transform.pcr.PcrTransformParams;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.hl7.fhir.instance.model.ProcedureRequest;
import org.hl7.fhir.instance.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcedureRequestTransformer extends AbstractTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(ProcedureRequestTransformer.class);

    public boolean shouldAlwaysTransform() {
        return true;
    }

    protected void transformResource(Long pcrId,
                          Resource resource,
                          AbstractPcrCsvWriter csvWriter,
                          PcrTransformParams params) throws Exception {

        ProcedureRequest fhir = (ProcedureRequest)resource;

//        long id;
//        long organisationId;
//        long patientId;
//        long personId;
//        Long encounterId = null;
//        Long practitionerId = null;
//        Date clinicalEffectiveDate = null;
//        Integer datePrecisionId = null;
//        Long snomedConceptId = null;
//        Integer procedureRequestStatusId = null;
//        String originalCode = null;
//        String originalTerm = null;
//
//        id = enterpriseId.longValue();
//        organisationId = params.getSubscriberOrganisationId().longValue();
//        patientId = params.getSubscriberPatientId().longValue();
//        personId = params.getSubscriberPersonId().longValue();
//
//        if (fhir.hasEncounter()) {
//            Reference encounterReference = fhir.getEncounter();
//            encounterId = findEnterpriseId(params, encounterReference);
//        }
//
//        if (fhir.hasOrderer()) {
//            Reference practitionerReference = fhir.getOrderer();
//            practitionerId = transformOnDemandAndMapId(practitionerReference, params);
//        }
//
//        if (fhir.hasScheduledDateTimeType()) {
//            DateTimeType dt = fhir.getScheduledDateTimeType();
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
//        if (fhir.hasStatus()) {
//            procedureRequestStatusId = new Integer(fhir.getStatus().ordinal());
//        }
//
//        org.endeavourhealth.transform.pcr.outputModels.ProcedureRequest model = (org.endeavourhealth.transform.pcr.outputModels.ProcedureRequest)csvWriter;
//        model.writeUpsert(id,
//            organisationId,
//            patientId,
//            personId,
//            encounterId,
//            practitionerId,
//            clinicalEffectiveDate,
//            datePrecisionId,
//            snomedConceptId,
//            procedureRequestStatusId,
//            originalCode,
//            originalTerm);
    }
}

