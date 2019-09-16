package org.endeavourhealth.transform.pcr.transforms;

import org.endeavourhealth.transform.pcr.PcrTransformParams;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.hl7.fhir.instance.model.Flag;
import org.hl7.fhir.instance.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlagTransformer extends AbstractTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(FlagTransformer.class);

    public boolean shouldAlwaysTransform() {
        return true;
    }

    protected void transformResource(Long pcrId,
                          Resource resource,
                          AbstractPcrCsvWriter csvWriter,
                          PcrTransformParams params) throws Exception {

        Flag fhir = (Flag)resource;

//        long id;
//        long organisationId;
//        long patientId;
//        long personId;
//        Date effectiveDate = null;
//        Integer datePrecisionId = null;
//        boolean isActive = true;
//        String flagText = null;
//
//        id = enterpriseId.longValue();
//        organisationId = params.getSubscriberOrganisationId().longValue();
//        patientId = params.getSubscriberPatientId().longValue();
//        personId = params.getSubscriberPersonId().longValue();
//
//        if (fhir.hasPeriod()) {
//            DateTimeType dt = fhir.getPeriod().getStartElement();
//            effectiveDate = dt.getValue();
//            datePrecisionId = convertDatePrecision(dt.getPrecision());
//        }
//
//        if (fhir.hasStatus()) {
//            isActive = (fhir.getStatus() == Flag.FlagStatus.ACTIVE);
//        }
//
//        if (fhir.hasCode()) {
//            flagText = fhir.getCode().getText();
//        }
//
//        org.endeavourhealth.transform.pcr.outputModels.Flag model
//                = (org.endeavourhealth.transform.pcr.outputModels.Flag)csvWriter;
//        model.writeUpsert(id,
//                organisationId,
//                patientId,
//                personId,
//                effectiveDate,
//                datePrecisionId,
//                isActive,
//                flagText);
    }
}
