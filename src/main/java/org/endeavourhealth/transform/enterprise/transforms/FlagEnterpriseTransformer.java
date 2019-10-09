package org.endeavourhealth.transform.enterprise.transforms;

import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.fhirStorage.FhirSerializationHelper;
import org.endeavourhealth.transform.enterprise.EnterpriseTransformHelper;
import org.endeavourhealth.transform.enterprise.outputModels.AbstractEnterpriseCsvWriter;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class FlagEnterpriseTransformer extends AbstractEnterpriseTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(FlagEnterpriseTransformer.class);

    @Override
    protected ResourceType getExpectedResourceType() {
        return ResourceType.Flag;
    }

    public boolean shouldAlwaysTransform() {
        return true;
    }

    protected void transformResource(Long enterpriseId,
                                     ResourceWrapper resourceWrapper,
                                     AbstractEnterpriseCsvWriter csvWriter,
                                     EnterpriseTransformHelper params) throws Exception {

        Flag fhir = (Flag)resourceWrapper.getResource(); //returns null if deleted

        //if deleted, confidential or the entire patient record shouldn't be there, then delete
        if (resourceWrapper.isDeleted()
                //|| isConfidential(fhir)
                || params.getShouldPatientRecordBeDeleted()) {
            csvWriter.writeDelete(enterpriseId.longValue());
            return;
        }

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

        isActive = fhir.hasStatus()
                && fhir.getStatus() == Flag.FlagStatus.ACTIVE;

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
