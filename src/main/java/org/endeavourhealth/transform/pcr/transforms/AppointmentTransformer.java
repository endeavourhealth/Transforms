package org.endeavourhealth.transform.pcr.transforms;

import org.endeavourhealth.transform.pcr.PcrTransformParams;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.hl7.fhir.instance.model.Appointment;
import org.hl7.fhir.instance.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class AppointmentTransformer extends AbstractTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(AppointmentTransformer.class);

    public boolean shouldAlwaysTransform() {
        return true;
    }

    protected void transformResource(Long pcrId,
                                     Resource resource,
                                     AbstractPcrCsvWriter csvWriter,
                                     PcrTransformParams params) throws Exception {

        Appointment fhir = (Appointment) resource;
        Long id = pcrId;
        Long appointmentScheduleId = null;
        Date slotStart = null;
        Date slotEnd = null;
        Long plannedDurationMinutes = null;
        Long typeConceptId = null;
        Long interactionConceptId = null;
        Long appointmentSlotId = null;
        Long patientId = null;
        Long enteredByPractitionerId = null;
        Date actualStartTime = null;
        Date actualEndTime = null;
        Long statusConceptId = null;
        Date eventTime = null;
        Long eventConceptId = null;
        Long organisationId = null;
        Long locationId = null;
        String description;
        Long specialityConceptId = null;
        Date scheduleStart = null;
        Date scheduleEnd = null;
        Long practitionerId = null;
        Boolean isMainPractitioner=false;

        if (fhir.hasSlot() && !fhir.getSlot().isEmpty()) {
           writeAppointmentSlot(id, appointmentScheduleId,slotStart,slotEnd,plannedDurationMinutes,typeConceptId,interactionConceptId,csvWriter);
        }

        Appointment.AppointmentStatus status = fhir.getStatus();

//        long id;
//        long organisationId;
//        long patientId;
//        Long practitionerId = null;
//        Long scheduleId = null;
//        Date startDate = null;
//        Integer plannedDuration = null;
//        Integer actualDuration = null;
//        int statusId;
//        Integer patientWait = null;
//        Integer patientDelay = null;
//        Date sentIn = null;
//        Date left = null;
//
//        if (fhir.hasParticipant()) {
//            for (Appointment.AppointmentParticipantComponent participantComponent: fhir.getParticipant()) {
//                Reference reference = participantComponent.getActor();
//                ReferenceComponents components = ReferenceHelper.getReferenceComponents(reference);
//
//                if (components.getResourceType() == ResourceType.Practitioner) {
//                    practitionerId = transformOnDemandAndMapId(reference, params);
//                }
//            }
//        }
//
//        //the test pack has data that refers to deleted or missing patients, so if we get a null
//        //patient ID here, then skip this resource
//        if (params.getEnterprisePatientId() == null) {
//            LOG.warn("Skipping " + fhir.getResourceType() + " " + fhir.getId() + " as no enterprise patient ID could be found for it");
//            return;
//        }
//
//        id = enterpriseId.longValue();
//        organisationId = params.getEnterpriseOrganisationId().longValue();
//        patientId = params.getEnterprisePatientId().longValue();
//        personId = params.getEnterprisePersonId().longValue();
//
//        if (fhir.getSlot().size() > 1) {
//            throw new TransformException("Cannot handle appointments linked to multiple slots " + fhir.getId());
//        }
//        Reference slotReference = fhir.getSlot().get(0);
//        Slot fhirSlot = (Slot)findResource(slotReference, params);
//        if (fhirSlot != null) {
//
//            Reference scheduleReference = fhirSlot.getSchedule();
//            scheduleId = transformOnDemandAndMapId(scheduleReference, params);
//
//        } else {
//            //a bug was found that meant this happened. So if it happens again, something is wrong
//            throw new TransformException("Failed to find " + slotReference.getReference() + " for " + fhir.getResourceType() + " " + fhir.getId());
//            //LOG.warn("Failed to find " + slotReference.getReference() + " for " + fhir.getResourceType() + " " + fhir.getId());
//        }
//
//        startDate = fhir.getStart();
//
//        Date end = fhir.getEnd();
//        if (startDate != null && end != null) {
//            long millisDiff = end.getTime() - startDate.getTime();
//            plannedDuration = Integer.valueOf((int)(millisDiff / (1000 * 60)));
//        }
//
//        if (fhir.hasMinutesDuration()) {
//            int duration = fhir.getMinutesDuration();
//            actualDuration = Integer.valueOf(duration);
//        }
//
//        Appointment.AppointmentStatus status = fhir.getStatus();
//        statusId = status.ordinal();
//
//        if (fhir.hasExtension()) {
//            for (Extension extension: fhir.getExtension()) {
//
//                if (extension.getUrl().equals(FhirExtensionUri.APPOINTMENT_PATIENT_WAIT)) {
//                    Duration d = (Duration)extension.getValue();
//                    if (!d.getUnit().equalsIgnoreCase("minutes")) {
//                        throw new TransformException("Unsupported patient wait unit [" + d.getUnit() + "] in " + fhir.getId());
//                    }
//                    int i = d.getValue().intValue();
//                    patientWait = Integer.valueOf(i);
//
//                } else if (extension.getUrl().equals(FhirExtensionUri.APPOINTMENT_PATIENT_DELAY)) {
//                    Duration d = (Duration)extension.getValue();
//                    if (!d.getUnit().equalsIgnoreCase("minutes")) {
//                        throw new TransformException("Unsupported patient delay unit [" + d.getUnit() + "] in " + fhir.getId());
//                    }
//                    int i = d.getValue().intValue();
//                    patientDelay = Integer.valueOf(i);
//
//                } else if (extension.getUrl().equals(FhirExtensionUri.APPOINTMENT_SENT_IN)) {
//                    DateTimeType dt = (DateTimeType)extension.getValue();
//                    sentIn = dt.getValue();
//
//                } else if (extension.getUrl().equals(FhirExtensionUri.APPOINTMENT_LEFT)) {
//                    DateTimeType dt = (DateTimeType)extension.getValue();
//                    left = dt.getValue();
//
//                }
//            }
//        }
//
//        org.endeavourhealth.transform.pcr.outputModels.Appointment model = (org.endeavourhealth.transform.pcr.outputModels.Appointment)csvWriter;
//        model.writeUpsert(id,
//            organisationId,
//            patientId,
//            personId,
//            practitionerId,
//            scheduleId,
//            startDate,
//            plannedDuration,
//            actualDuration,
//            statusId,
//            patientWait,
//            patientDelay,
//            sentIn,
//            left);
    }

    private void writeAppointmentSlot(Long id,
                                      Long appointmentScheduleId,
                                      Date slotStart,
                                      Date slotEnd,
                                      Long plannedDurationMinutes,
                                      Long typeConceptId,
                                      Long interactionConceptId,
                                      AbstractPcrCsvWriter csvWriter) throws Exception {
        org.endeavourhealth.transform.pcr.outputModels.AppointmentSlot model =
                (org.endeavourhealth.transform.pcr.outputModels.AppointmentSlot) csvWriter;
        model.writeUpsert(id, appointmentScheduleId, slotStart, slotEnd, plannedDurationMinutes, typeConceptId, interactionConceptId);
    }

    private void writeAppointAttendance(Long appointmentSlotId,
                                        Long patientId,
                                        Long enteredByPractitionerId,
                                        Date actualStartTime,
                                        Date actualEndTime,
                                        Long statusConceptId, AbstractPcrCsvWriter csvWriter) throws Exception {
        org.endeavourhealth.transform.pcr.outputModels.AppointmentAttendance model =
                (org.endeavourhealth.transform.pcr.outputModels.AppointmentAttendance) csvWriter;
        model.writeUpsert(appointmentSlotId, patientId, enteredByPractitionerId, actualStartTime, actualEndTime, statusConceptId);
    }

    private void writeAppointAttendanceEvent(Long appointmentSlotId,
                                             Date eventTime,
                                             Long eventConceptId,
                                             Long enteredByPractitionerId,
                                             AbstractPcrCsvWriter csvWriter
    ) throws Exception {
        org.endeavourhealth.transform.pcr.outputModels.AppointmentAttendanceEvent model =
                (org.endeavourhealth.transform.pcr.outputModels.AppointmentAttendanceEvent) csvWriter;
        model.writeUpsert(appointmentSlotId, eventTime, eventConceptId, enteredByPractitionerId);
    }

    private void AppointmentSchedule(Long id,
                                     Long organisationId,
                                     Long locationId,
                                     String description,
                                     Long typeConceptId,
                                     Long specialityConceptId,
                                     Date scheduleStart,
                                     Date scheduleEnd,
                                     AbstractPcrCsvWriter csvWriter) throws Exception {
        org.endeavourhealth.transform.pcr.outputModels.AppointmentSchedule model =
                (org.endeavourhealth.transform.pcr.outputModels.AppointmentSchedule) csvWriter;
        model.writeUpsert(id, organisationId, locationId, description, typeConceptId, specialityConceptId, scheduleStart, scheduleEnd);
    }

    private void writeAppointmentSchedulePractitioner(Long appointmentScheduleId,
                                                      Long practitionerId,
                                                      Long enteredByPractitionerId,
                                                      Boolean isMainPractitioner,
                                                      AbstractPcrCsvWriter csvWriter
    ) throws Exception {
        org.endeavourhealth.transform.pcr.outputModels.AppointmentSchedulePractitioner model =
                (org.endeavourhealth.transform.pcr.outputModels.AppointmentSchedulePractitioner) csvWriter;
        model.writeUpsert(appointmentScheduleId, practitionerId, enteredByPractitionerId, isMainPractitioner);
    }
}

