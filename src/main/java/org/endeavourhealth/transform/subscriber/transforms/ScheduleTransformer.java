package org.endeavourhealth.transform.subscriber.transforms;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirResourceHelper;
import org.endeavourhealth.transform.subscriber.SubscriberTransformHelper;
import org.endeavourhealth.transform.subscriber.targetTables.SubscriberTableId;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class ScheduleTransformer extends AbstractSubscriberTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ScheduleTransformer.class);

    @Override
    protected ResourceType getExpectedResourceType() {
        return ResourceType.Schedule;
    }

    public boolean shouldAlwaysTransform() {
        return false;
    }

    @Override
    protected void transformResource(SubscriberId subscriberId, ResourceWrapper resourceWrapper, SubscriberTransformHelper params) throws Exception {

        org.endeavourhealth.transform.subscriber.targetTables.Schedule model = params.getOutputContainer().getSchedules();

        if (resourceWrapper.isDeleted()) {
            model.writeDelete(subscriberId);
            return;
        }

        Schedule fhir = (Schedule)resourceWrapper.getResource();

        long organisationId;
        Long practitionerId = null;
        Date startDate = null;
        String type = null;
        String location = null;
        String name = null;

        organisationId = params.getSubscriberOrganisationId().longValue();

        if (fhir.hasActor()) {
            Reference practitionerReference = fhir.getActor();
            practitionerId = transformOnDemandAndMapId(practitionerReference, SubscriberTableId.PRACTITIONER, params);
        }

        if (fhir.hasPlanningHorizon()) {
            Period period = fhir.getPlanningHorizon();
            startDate = period.getStart();
        }

        if (fhir.hasExtension()) {
            for (Extension extension: fhir.getExtension()) {
                if (extension.getUrl().equals(FhirExtensionUri.SCHEDULE_LOCATION)) {
                    Reference locationReference = (Reference)extension.getValue();

                    Location fhirLocation = (Location)params.findOrRetrieveResource(locationReference);
                    if (fhirLocation != null) {
                        location = fhirLocation.getName();
                    }
                }
            }
        }
        //in the absence of a specific named location, see if we have a location TYPE set
        if (Strings.isNullOrEmpty(location)) {
            StringType stringType = (StringType)ExtensionConverter.findExtensionValue(fhir, FhirExtensionUri.SCHEDULE_LOCATION_TYPE);
            if (stringType != null) {
                location = stringType.toString();
            }
        }

        if (fhir.hasType()) {

            //all known and expected data has just a single type, but add this check just in case
            if (fhir.getType().size() > 1) {
                throw new TransformException("Enterprise ScheduleTransformer doesn't support Schedules with multiple types");
            }

            for (CodeableConcept typeCodeableConcept: fhir.getType()) {
                type = typeCodeableConcept.getText();
            }
        }

        Extension scheduleNameRef = ExtensionConverter.findExtension(fhir, FhirExtensionUri.SCHEDULE_NAME);
        if (scheduleNameRef != null) {
            StringType ref = (StringType) scheduleNameRef.getValue();
            if (ref != null && ref.hasValue()) {
                name = ref.getValue();
            }
        }


        model.writeUpsert(subscriberId,
            organisationId,
            practitionerId,
            startDate,
            type,
            location,
            name);


    }

    @Override
    protected SubscriberTableId getMainSubscriberTableId() {
        return SubscriberTableId.SCHEDULE;
    }
}
