package org.endeavourhealth.transform.subscriber.transforms;

import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.subscriber.SubscriberTransformParams;
import org.endeavourhealth.transform.subscriber.outputModels.AbstractSubscriberCsvWriter;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class ScheduleTransformer extends AbstractTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(ScheduleTransformer.class);

    public boolean shouldAlwaysTransform() {
        return false;
    }

    protected void transformResource(Long enterpriseId,
                                     Resource resource,
                                     AbstractSubscriberCsvWriter csvWriter,
                                     SubscriberTransformParams params) throws Exception {

        Schedule fhir = (Schedule)resource;

        long id;
        long organisationId;
        Long practitionerId = null;
        Date startDate = null;
        String type = null;
        String location = null;
        String comments = null;

        id = enterpriseId.longValue();
        organisationId = params.getEnterpriseOrganisationId().longValue();

        if (fhir.hasActor()) {
            Reference practitionerReference = fhir.getActor();
            practitionerId = transformOnDemandAndMapId(practitionerReference, params);
        }

        if (fhir.hasPlanningHorizon()) {
            Period period = fhir.getPlanningHorizon();
            startDate = period.getStart();
        }

        if (fhir.hasExtension()) {
            for (Extension extension: fhir.getExtension()) {
                if (extension.getUrl().equals(FhirExtensionUri.SCHEDULE_LOCATION)) {
                    Reference locationReference = (Reference)extension.getValue();

                    Location fhirLocation = (Location)findResource(locationReference, params);
                    if (fhirLocation != null) {
                        location = fhirLocation.getName();
                    }
                }
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

        comments = fhir.getComment();

        org.endeavourhealth.transform.subscriber.outputModels.Schedule model
                = (org.endeavourhealth.transform.subscriber.outputModels.Schedule)csvWriter;
        model.writeUpsert(id,
            organisationId,
            practitionerId,
            startDate,
            type,
            location,
            comments);
    }
}
