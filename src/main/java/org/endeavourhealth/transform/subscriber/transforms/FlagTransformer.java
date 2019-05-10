package org.endeavourhealth.transform.subscriber.transforms;

import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.core.fhirStorage.FhirResourceHelper;
import org.endeavourhealth.transform.subscriber.SubscriberTransformParams;
import org.endeavourhealth.transform.subscriber.targetTables.SubscriberTableId;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.Flag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class FlagTransformer extends AbstractSubscriberTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(FlagTransformer.class);

    public boolean shouldAlwaysTransform() {
        return true;
    }

    @Override
    protected void transformResource(SubscriberId subscriberId, ResourceWrapper resourceWrapper, SubscriberTransformParams params) throws Exception {

        org.endeavourhealth.transform.subscriber.targetTables.Flag model = params.getOutputContainer().getFlags();

        if (resourceWrapper.isDeleted()) {
            model.writeDelete(subscriberId);

            return;
        }

        Flag fhir = (Flag) FhirResourceHelper.deserialiseResouce(resourceWrapper);

        long id;
        long organisationId;
        long patientId;
        long personId;
        Date effectiveDate = null;
        Integer datePrecisionConceptId = null;
        boolean isActive = true;
        String flagText = null;

        id = subscriberId.getSubscriberId();
        organisationId = params.getEnterpriseOrganisationId().longValue();
        patientId = params.getEnterprisePatientId().longValue();
        personId = params.getEnterprisePersonId().longValue();

        if (fhir.hasPeriod()) {
            DateTimeType dt = fhir.getPeriod().getStartElement();
            effectiveDate = dt.getValue();
            datePrecisionConceptId = convertDatePrecision(params, fhir, dt.getPrecision());
        }

        if (fhir.hasStatus()) {
            isActive = (fhir.getStatus() == Flag.FlagStatus.ACTIVE);
        }

        if (fhir.hasCode()) {
            flagText = fhir.getCode().getText();
        }

        model.writeUpsert(subscriberId,
                organisationId,
                patientId,
                personId,
                effectiveDate,
                datePrecisionConceptId,
                isActive,
                flagText);


    }

    @Override
    protected SubscriberTableId getMainSubscriberTableId() {
        return SubscriberTableId.FLAG;
    }


}
