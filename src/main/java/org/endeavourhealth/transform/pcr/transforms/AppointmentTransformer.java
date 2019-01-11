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

