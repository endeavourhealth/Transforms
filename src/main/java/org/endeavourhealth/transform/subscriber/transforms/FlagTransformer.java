package org.endeavourhealth.transform.subscriber.transforms;

import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.core.fhirStorage.FhirResourceHelper;
import org.endeavourhealth.transform.subscriber.SubscriberTransformHelper;
import org.endeavourhealth.transform.subscriber.targetTables.SubscriberTableId;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.Flag;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class FlagTransformer extends AbstractSubscriberTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(FlagTransformer.class);

    @Override
    protected ResourceType getExpectedResourceType() {
        return ResourceType.Flag;
    }

    public boolean shouldAlwaysTransform() {
        return true;
    }

    @Override
    protected void transformResource(SubscriberId subscriberId, ResourceWrapper resourceWrapper, SubscriberTransformHelper params) throws Exception {

        org.endeavourhealth.transform.subscriber.targetTables.Flag model = params.getOutputContainer().getFlags();

        Flag fhir = (Flag)resourceWrapper.getResource(); //returns null if deleted

        //if deleted, confidential or the entire patient record shouldn't be there, then delete
        if (resourceWrapper.isDeleted()
                //|| isConfidential(fhir)
                || params.getShouldPatientRecordBeDeleted()) {
            model.writeDelete(subscriberId);
            return;
        }

        long organisationId;
        long patientId;
        long personId;
        Date effectiveDate = null;
        Integer datePrecisionConceptId = null;
        boolean isActive = true;
        String flagText = null;

        organisationId = params.getSubscriberOrganisationId().longValue();
        patientId = params.getSubscriberPatientId().longValue();
        personId = params.getSubscriberPersonId().longValue();

        if (fhir.hasPeriod()) {
            DateTimeType dt = fhir.getPeriod().getStartElement();
            effectiveDate = dt.getValue();
            if (dt.getPrecision() != null) {
                datePrecisionConceptId = convertDatePrecision(params, fhir, dt.getPrecision(), effectiveDate.toString());
            }
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
