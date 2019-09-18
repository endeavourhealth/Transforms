package org.endeavourhealth.transform.subscriber.transforms;

import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.ReferenceComponents;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.ehr.models.ResourceWrapper;
import org.endeavourhealth.core.database.dal.subscriberTransform.models.SubscriberId;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.core.fhirStorage.FhirResourceHelper;
import org.endeavourhealth.transform.subscriber.IMConstant;
import org.endeavourhealth.transform.subscriber.IMHelper;
import org.endeavourhealth.transform.subscriber.SubscriberTransformHelper;
import org.endeavourhealth.transform.subscriber.targetTables.SubscriberTableId;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class AppointmentTransformer extends AbstractSubscriberTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(AppointmentTransformer.class);

    @Override
    protected ResourceType getExpectedResourceType() {
        return ResourceType.Appointment;
    }

    public boolean shouldAlwaysTransform() {
        return true;
    }

    @Override
    protected void transformResource(SubscriberId subscriberId, ResourceWrapper resourceWrapper, SubscriberTransformHelper params) throws Exception {

        org.endeavourhealth.transform.subscriber.targetTables.Appointment model = params.getOutputContainer().getAppointments();

        Appointment fhir = (Appointment)resourceWrapper.getResource(); //returns null if deleted

        //if deleted, confidential or the entire patient record shouldn't be there, then delete
        if (resourceWrapper.isDeleted()
                //|| isConfidential(fhir)
                || params.getShouldPatientRecordBeDeleted()) {
            model.writeDelete(subscriberId);
            return;
        }

        long id;
        long organizationId;
        long patientId;
        long personId;
        Long practitionerId = null;
        Long scheduleId = null;
        Date startDate = null;
        Integer plannedDuration = null;
        Integer actualDuration = null;
        Integer appointmentStatusConceptId;
        Integer patientWait = null;
        Integer patientDelay = null;
        Date sentIn = null;
        Date left = null;
        String sourceId = null;
        Date cancelledDate = null;

        if (fhir.hasParticipant()) {
            for (Appointment.AppointmentParticipantComponent participantComponent : fhir.getParticipant()) {
                Reference reference = participantComponent.getActor();
                ReferenceComponents components = ReferenceHelper.getReferenceComponents(reference);

                if (components.getResourceType() == ResourceType.Practitioner) {
                    practitionerId = transformOnDemandAndMapId(reference, SubscriberTableId.PRACTITIONER, params);
                }
            }
        }

        //the test pack has data that refers to deleted or missing patients, so if we get a null
        //patient ID here, then skip this resource
        if (params.getSubscriberPatientId() == null) {
            LOG.warn("Skipping " + fhir.getResourceType() + " " + fhir.getId() + " as no Enterprise patient ID could be found for it");
            return;
        }

        id = subscriberId.getSubscriberId();
        organizationId = params.getSubscriberOrganisationId().longValue();
        patientId = params.getSubscriberPatientId().longValue();
        personId = params.getSubscriberPersonId().longValue();

        if (fhir.getSlot().size() > 1) {
            throw new TransformException("Cannot handle appointments linked to multiple slots " + fhir.getId());
        }
        if (fhir.getSlot().size() > 0) {
            Reference slotReference = fhir.getSlot().get(0);
            Slot fhirSlot = (Slot)params.findOrRetrieveResource(slotReference);
            if (fhirSlot != null) {

                Reference scheduleReference = fhirSlot.getSchedule();
                scheduleId = transformOnDemandAndMapId(scheduleReference, SubscriberTableId.SCHEDULE, params);

            } else {
                //a bug was found that meant this happened. So if it happens again, something is wrong
                throw new TransformException("Failed to find " + slotReference.getReference() + " for " + fhir.getResourceType() + " " + fhir.getId());
                //LOG.warn("Failed to find " + slotReference.getReference() + " for " + fhir.getResourceType() + " " + fhir.getId());
            }
        }

        startDate = fhir.getStart();

        Date end = fhir.getEnd();
        if (startDate != null && end != null) {
            long millisDiff = end.getTime() - startDate.getTime();
            plannedDuration = Integer.valueOf((int) (millisDiff / (1000 * 60)));
        }

        if (fhir.hasMinutesDuration()) {
            int duration = fhir.getMinutesDuration();
            actualDuration = Integer.valueOf(duration);
        }

        Appointment.AppointmentStatus status = fhir.getStatus();
        appointmentStatusConceptId = IMHelper.getIMConcept(params, fhir, IMConstant.FHIR_APPOINTMENT_STATUS, status.toCode(), status.getDisplay());

        if (fhir.hasExtension()) {
            for (Extension extension : fhir.getExtension()) {

                if (extension.getUrl().equals(FhirExtensionUri.APPOINTMENT_PATIENT_WAIT)) {
                    Duration d = (Duration) extension.getValue();
                    if (!d.getUnit().equalsIgnoreCase("minutes")) {
                        throw new TransformException("Unsupported patient wait unit [" + d.getUnit() + "] in " + fhir.getId());
                    }
                    int i = d.getValue().intValue();
                    patientWait = Integer.valueOf(i);

                } else if (extension.getUrl().equals(FhirExtensionUri.APPOINTMENT_PATIENT_DELAY)) {
                    Duration d = (Duration) extension.getValue();
                    if (!d.getUnit().equalsIgnoreCase("minutes")) {
                        throw new TransformException("Unsupported patient delay unit [" + d.getUnit() + "] in " + fhir.getId());
                    }
                    int i = d.getValue().intValue();
                    patientDelay = Integer.valueOf(i);

                } else if (extension.getUrl().equals(FhirExtensionUri.APPOINTMENT_SENT_IN)) {
                    DateTimeType dt = (DateTimeType) extension.getValue();
                    sentIn = dt.getValue();

                } else if (extension.getUrl().equals(FhirExtensionUri.APPOINTMENT_LEFT)) {
                    DateTimeType dt = (DateTimeType) extension.getValue();
                    left = dt.getValue();

                } else if (extension.getUrl().equals(FhirExtensionUri.APPOINTMENT_ORIGINAL_IDENTIFIER)) {

                    StringType orig_id = (StringType) extension.getValue();
                    //Identifier orig_id = (Identifier) extension.getValue(); //AppointmentBuilder uses String
                    sourceId = orig_id.getValue();

                } else if (extension.getUrl().equals(FhirExtensionUri.APPOINTMENT_CANCELLATION_DATE)) {
                    DateTimeType dt = (DateTimeType) extension.getValue();
                    cancelledDate = dt.getValue();
                }
            }
        }

        model.writeUpsert(subscriberId,
                organizationId,
                patientId,
                personId,
                practitionerId,
                scheduleId,
                startDate,
                plannedDuration,
                actualDuration,
                appointmentStatusConceptId,
                patientWait,
                patientDelay,
                sentIn,
                left,
                sourceId,
                cancelledDate);

    }

    @Override
    protected SubscriberTableId getMainSubscriberTableId() {
        return SubscriberTableId.APPOINTMENT;
    }


}
