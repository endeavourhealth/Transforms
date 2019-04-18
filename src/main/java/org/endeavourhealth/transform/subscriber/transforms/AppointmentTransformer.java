package org.endeavourhealth.transform.subscriber.transforms;

import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.ReferenceComponents;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.subscriber.SubscriberTransformParams;
import org.endeavourhealth.transform.subscriber.outputModels.AbstractSubscriberCsvWriter;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class AppointmentTransformer extends AbstractTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(AppointmentTransformer.class);

    public boolean shouldAlwaysTransform() {
        return true;
    }

    protected void transformResource(Long enterpriseId,
                                     Resource resource,
                                     AbstractSubscriberCsvWriter csvWriter,
                                     SubscriberTransformParams params) throws Exception {

        Appointment fhir = (Appointment)resource;

        long id;
        long organisationId;
        long patientId;
        long personId;
        Long practitionerId = null;
        Long scheduleId = null;
        Date startDate = null;
        Integer plannedDuration = null;
        Integer actualDuration = null;
        int statusId;
        Integer patientWait = null;
        Integer patientDelay = null;
        Date sentIn = null;
        Date left = null;
        String sourceId = null;

        if (fhir.hasParticipant()) {
            for (Appointment.AppointmentParticipantComponent participantComponent: fhir.getParticipant()) {
                Reference reference = participantComponent.getActor();
                ReferenceComponents components = ReferenceHelper.getReferenceComponents(reference);

                if (components.getResourceType() == ResourceType.Practitioner) {
                    practitionerId = transformOnDemandAndMapId(reference, params);
                }
            }
        }

        //the test pack has data that refers to deleted or missing patients, so if we get a null
        //patient ID here, then skip this resource
        if (params.getEnterprisePatientId() == null) {
            LOG.warn("Skipping " + fhir.getResourceType() + " " + fhir.getId() + " as no Enterprise patient ID could be found for it");
            return;
        }

        id = enterpriseId.longValue();
        organisationId = params.getEnterpriseOrganisationId().longValue();
        patientId = params.getEnterprisePatientId().longValue();
        personId = params.getEnterprisePersonId().longValue();

        if (fhir.getSlot().size() > 1) {
            throw new TransformException("Cannot handle appointments linked to multiple slots " + fhir.getId());
        }
        Reference slotReference = fhir.getSlot().get(0);
        Slot fhirSlot = (Slot)findResource(slotReference, params);
        if (fhirSlot != null) {

            Reference scheduleReference = fhirSlot.getSchedule();
            scheduleId = transformOnDemandAndMapId(scheduleReference, params);

        } else {
            //a bug was found that meant this happened. So if it happens again, something is wrong
            throw new TransformException("Failed to find " + slotReference.getReference() + " for " + fhir.getResourceType() + " " + fhir.getId());
            //LOG.warn("Failed to find " + slotReference.getReference() + " for " + fhir.getResourceType() + " " + fhir.getId());
        }

        startDate = fhir.getStart();

        Date end = fhir.getEnd();
        if (startDate != null && end != null) {
            long millisDiff = end.getTime() - startDate.getTime();
            plannedDuration = Integer.valueOf((int)(millisDiff / (1000 * 60)));
        }

        if (fhir.hasMinutesDuration()) {
            int duration = fhir.getMinutesDuration();
            actualDuration = Integer.valueOf(duration);
        }

        Appointment.AppointmentStatus status = fhir.getStatus();
        statusId = status.ordinal();

        if (fhir.hasExtension()) {
            for (Extension extension: fhir.getExtension()) {

                if (extension.getUrl().equals(FhirExtensionUri.APPOINTMENT_PATIENT_WAIT)) {
                    Duration d = (Duration)extension.getValue();
                    if (!d.getUnit().equalsIgnoreCase("minutes")) {
                        throw new TransformException("Unsupported patient wait unit [" + d.getUnit() + "] in " + fhir.getId());
                    }
                    int i = d.getValue().intValue();
                    patientWait = Integer.valueOf(i);

                } else if (extension.getUrl().equals(FhirExtensionUri.APPOINTMENT_PATIENT_DELAY)) {
                    Duration d = (Duration)extension.getValue();
                    if (!d.getUnit().equalsIgnoreCase("minutes")) {
                        throw new TransformException("Unsupported patient delay unit [" + d.getUnit() + "] in " + fhir.getId());
                    }
                    int i = d.getValue().intValue();
                    patientDelay = Integer.valueOf(i);

                } else if (extension.getUrl().equals(FhirExtensionUri.APPOINTMENT_SENT_IN)) {
                    DateTimeType dt = (DateTimeType)extension.getValue();
                    sentIn = dt.getValue();

                } else if (extension.getUrl().equals(FhirExtensionUri.APPOINTMENT_LEFT)) {
                    DateTimeType dt = (DateTimeType)extension.getValue();
                    left = dt.getValue();

                } else if (extension.getUrl().equals(FhirExtensionUri.APPOINTMENT_ORIGINAL_IDENTIFIER)) {
                    Identifier orig_id = (Identifier) extension.getValue();
                    sourceId = orig_id.getValue();
                }
            }
        }

        org.endeavourhealth.transform.subscriber.outputModels.Appointment model
                = (org.endeavourhealth.transform.subscriber.outputModels.Appointment)csvWriter;
        model.writeUpsert(id,
            organisationId,
            patientId,
            personId,
            practitionerId,
            scheduleId,
            startDate,
            plannedDuration,
            actualDuration,
            statusId,
            patientWait,
            patientDelay,
            sentIn,
            left,
            sourceId);
    }



}

