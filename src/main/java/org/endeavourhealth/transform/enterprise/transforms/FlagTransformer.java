package org.endeavourhealth.transform.enterprise.transforms;

import org.endeavourhealth.transform.enterprise.EnterpriseTransformParams;
import org.endeavourhealth.transform.enterprise.outputModels.AbstractEnterpriseCsvWriter;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.Flag;
import org.hl7.fhir.instance.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class FlagTransformer extends AbstractTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(FlagTransformer.class);

    public boolean shouldAlwaysTransform() {
        return true;
    }

    protected void transformResource(Long enterpriseId,
                          Resource resource,
                          AbstractEnterpriseCsvWriter csvWriter,
                          EnterpriseTransformParams params) throws Exception {

        Flag fhir = (Flag)resource;

        long id;
        long organisationId;
        long patientId;
        long personId;
        Date effectiveDate = null;
        Integer datePrecisionId = null;
        boolean isActive = true;
        String flagText = null;

        id = enterpriseId.longValue();
        organisationId = params.getEnterpriseOrganisationId().longValue();
        patientId = params.getEnterprisePatientId().longValue();
        personId = params.getEnterprisePersonId().longValue();

        if (fhir.hasPeriod()) {
            DateTimeType dt = fhir.getPeriod().getStartElement();
            effectiveDate = dt.getValue();
            datePrecisionId = convertDatePrecision(dt.getPrecision());
        }

        if (fhir.hasStatus()) {
            isActive = (fhir.getStatus() == Flag.FlagStatus.ACTIVE);
        }

        if (fhir.hasCode()) {
            flagText = fhir.getCode().getText();
        }

        org.endeavourhealth.transform.enterprise.outputModels.Flag model
                = (org.endeavourhealth.transform.enterprise.outputModels.Flag)csvWriter;
        model.writeUpsert(id,
                organisationId,
                patientId,
                personId,
                effectiveDate,
                datePrecisionId,
                isActive,
                flagText);
    }
}