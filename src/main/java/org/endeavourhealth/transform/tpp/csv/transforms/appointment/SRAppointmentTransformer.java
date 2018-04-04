package org.endeavourhealth.transform.tpp.csv.transforms.appointment;

import org.endeavourhealth.core.database.dal.publisherTransform.models.TppMappingRef;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.AppointmentBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.SlotBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.cache.AppointmentResourceCache;
import org.endeavourhealth.transform.tpp.cache.SlotResourceCache;
import org.endeavourhealth.transform.tpp.csv.schema.appointment.SRAppointment;
import org.hl7.fhir.instance.model.Appointment;
import org.hl7.fhir.instance.model.Reference;

import java.util.Date;
import java.util.Map;

public class SRAppointmentTransformer {

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRAppointment.class);
        while (parser.nextRecord()) {

            try {
                createResource((SRAppointment)parser, fhirResourceFiler, csvHelper);
            } catch (Exception ex) {
                fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
            }
        }
    }

    private static void createResource(SRAppointment parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper) throws Exception {

        CsvCell appointmentId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();

        //use the same Id reference for the Appointment and the Slot; since it's a different resource type, it should be fine
        AppointmentBuilder appointmentBuilder
                = AppointmentResourceCache.getAppointmentBuilder(appointmentId, csvHelper, fhirResourceFiler);
        SlotBuilder slotBuilder
                = SlotResourceCache.getSlotBuilder(appointmentId, csvHelper, fhirResourceFiler);

        Reference patientReference = csvHelper.createPatientReference(patientId);
        appointmentBuilder.addParticipant(patientReference, Appointment.ParticipationStatus.ACCEPTED, patientId);

        CsvCell rotaId = parser.getIDRota();
        if (!rotaId.isEmpty()) {
            Reference scheduleReference = csvHelper.createScheduleReference(rotaId);
            slotBuilder.setSchedule(scheduleReference, rotaId);
        }

        //because we're only storing slots with patients, all slots are "busy"
        slotBuilder.setFreeBusyType(org.hl7.fhir.instance.model.Slot.SlotStatus.BUSY);

        //cell is both date and time, so create datetime from both
        CsvCell startDate = parser.getDateStart();
        CsvCell startTime = parser.getDateStart();
        Date startDateTime = CsvCell.getDateTimeFromTwoCells(startDate, startTime);
        slotBuilder.setStartDateTime(startDateTime, startDate);
        appointmentBuilder.setStartDateTime(startDateTime, startDate);

        CsvCell endDate = parser.getDateEnd();
        CsvCell endTime = parser.getDateEnd();
        Date endDateTime = CsvCell.getDateTimeFromTwoCells(endDate, endTime);
        slotBuilder.setEndDateTime(endDateTime, endDate);
        appointmentBuilder.setEndDateTime(endDateTime, endDate);

        long durationMillis = endDateTime.getTime() - startDateTime.getTime();
        int durationMins = (int)(durationMillis / 1000 / 60);
        appointmentBuilder.setMinutesDuration(durationMins);

        Reference slotReference = csvHelper.createSlotReference(appointmentId);
        appointmentBuilder.addSlot(slotReference, appointmentId);

        CsvCell appointmentStaffProfileId = parser.getIDProfileClinician();
        if (!appointmentStaffProfileId.isEmpty()) {
            //TODO:  this links to SRStaffMemberProfile -> how get staff reference?
            //Reference practitionerReference = ReferenceHelper.createReference(ResourceType.Practitioner, appointmentStaffProfileId.getString());
            //appointmentBuilder.addParticipant(practitionerReference, Appointment.ParticipationStatus.ACCEPTED, appointmentStaffProfileId);
        }

        CsvCell patientSeenDate = parser.getDatePatientSeen();
        CsvCell patientSeenTime = parser.getDatePatientSeen();
        if (!patientSeenDate.isEmpty()) {
            Date seenDateTime = CsvCell.getDateTimeFromTwoCells(patientSeenDate, patientSeenTime);
            appointmentBuilder.setSentInDateTime(seenDateTime, patientSeenDate);
        }

        CsvCell appointmentStatus = parser.getAppointmentStatus();
        if (!appointmentStatus.isEmpty() && appointmentStatus.getLong()>0) {

            TppMappingRef tppMappingRef = csvHelper.lookUpTppMappingRef(appointmentStatus.getLong());
            String statusTerm = tppMappingRef.getMappedTerm();
            Appointment.AppointmentStatus status = convertAppointmentStatus (statusTerm);
            appointmentBuilder.setStatus(status, appointmentStatus);
        }
    }

    private static Appointment.AppointmentStatus convertAppointmentStatus(String status) {

        if (status.equalsIgnoreCase("did not attend")) {
            return Appointment.AppointmentStatus.NOSHOW;
        } else if (status.toLowerCase().startsWith("cancelled")) {
            return Appointment.AppointmentStatus.CANCELLED;
        } else if (status.equalsIgnoreCase("finished")) {
            return Appointment.AppointmentStatus.FULFILLED;
        } else if (status.equalsIgnoreCase("arrived")) {
            return Appointment.AppointmentStatus.ARRIVED;
        } else if (status.equalsIgnoreCase("in progress")) {
            return Appointment.AppointmentStatus.BOOKED;
        } else if (status.equalsIgnoreCase("waiting")) {
            return Appointment.AppointmentStatus.PENDING;
        } else if (status.toLowerCase().startsWith("booked")) {
            return Appointment.AppointmentStatus.BOOKED;
        } else {
            return Appointment.AppointmentStatus.NULL;
        }
    }
}
