package org.endeavourhealth.transform.tpp.csv.transforms.appointment;

import org.endeavourhealth.core.database.dal.publisherCommon.models.TppMappingRef;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.AppointmentBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.SlotBuilder;
import org.endeavourhealth.transform.tpp.cache.AppointmentFlagCache;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.appointment.SRAppointment;
import org.hl7.fhir.instance.model.Appointment;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.hl7.fhir.instance.model.Slot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class SRAppointmentTransformer {

    // FHIR filing note: we create a new slot from constructor, therefore we have to do a
    // mapId on slot, therefore we need to use local ids for appointment as well.

    private static final Logger LOG = LoggerFactory.getLogger(SRAppointmentTransformer.class);


    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRAppointment.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createResource((SRAppointment) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    private static void createResource(SRAppointment parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper) throws Exception {

        CsvCell appointmentIdCell = parser.getRowIdentifier();
        CsvCell patientIdCell = parser.getIDPatient();
        CsvCell deleteDataCell = parser.getRemovedData();

        if (deleteDataCell != null //cell wasn't present on old versions
                && deleteDataCell.getIntAsBoolean()) {

            // get previously filed resources for deletion
            Appointment appointment = (Appointment) csvHelper.retrieveResource(appointmentIdCell.getString(), ResourceType.Appointment);
            if (appointment != null) {

                //create the linked slot to delete using the same Id as the appointment. nb. there is no patient on
                //a slot so we don't need to retrieve the existing resource.  mapIds needs to be the default true;
                SlotBuilder slotBuilder = new SlotBuilder();
                slotBuilder.setId(appointmentIdCell.getString(), appointmentIdCell);
                slotBuilder.setDeletedAudit(deleteDataCell);
                fhirResourceFiler.deletePatientResource(parser.getCurrentState(), slotBuilder);

                //call the resource deletion on the appointment builder with mapIds = false, as it was retrieved from the DB
                AppointmentBuilder appointmentBuilder = new AppointmentBuilder(appointment);
                appointmentBuilder.setDeletedAudit(deleteDataCell);
                fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, appointmentBuilder);
            }
            return;
        }

        // If we don't have a patient reference, don't file the slot as the filer doesn't support saving slots without a patient
        if (patientIdCell.isEmpty()) {
            return;
        }

        //use the same Id reference for the Appointment and the Slot; since it's a different resource type, it should be fine
        AppointmentBuilder appointmentBuilder = new AppointmentBuilder();
        appointmentBuilder.setId(appointmentIdCell.getString(), appointmentIdCell);
        appointmentBuilder.setOriginalIdentifier(appointmentIdCell.getString(), appointmentIdCell);

        SlotBuilder slotBuilder = new SlotBuilder();
        slotBuilder.setId(appointmentIdCell.getString(), appointmentIdCell);

        Reference patientReference = csvHelper.createPatientReference(patientIdCell);
        appointmentBuilder.addParticipant(patientReference, Appointment.ParticipationStatus.ACCEPTED, patientIdCell);

        Reference slotRef = csvHelper.createSlotReference(appointmentIdCell);
        appointmentBuilder.addSlot(slotRef, appointmentIdCell);

        CsvCell rotaId = parser.getIDRota();
        if (!rotaId.isEmpty()) {
            Reference scheduleReference = csvHelper.createScheduleReference(rotaId);
            slotBuilder.setSchedule(scheduleReference, rotaId);
        }

        //because we're only storing slots with patients, all slots are "busy"
        slotBuilder.setFreeBusyType(Slot.SlotStatus.BUSY);

        //cell is both date and time, so create datetime from both
        CsvCell startDateCell = parser.getDateStart();
        Date startDateTime = null;
        if (!startDateCell.isEmpty()) {
            startDateTime = startDateCell.getDateTime();
            slotBuilder.setStartDateTime(startDateTime, startDateCell);
            appointmentBuilder.setStartDateTime(startDateTime, startDateCell);
        }

        CsvCell endDateCell = parser.getDateEnd();
        Date endDateTime = null;
        if (!endDateCell.isEmpty()) {
            endDateTime = endDateCell.getDateTime();
            slotBuilder.setEndDateTime(endDateTime, endDateCell);
            appointmentBuilder.setEndDateTime(endDateTime, endDateCell);
        }

        if (endDateTime != null && startDateTime != null) {
            long durationMillis = endDateTime.getTime() - startDateTime.getTime();
            int durationMins = (int) (durationMillis / 1000 / 60);
            appointmentBuilder.setMinutesDuration(durationMins);
        }

        CsvCell profileIdClinician = parser.getIDProfileClinician();
        if (!profileIdClinician.isEmpty()) {
            Reference practitionerReference = csvHelper.createPractitionerReferenceForProfileId(profileIdClinician);
            appointmentBuilder.addParticipant(practitionerReference, Appointment.ParticipationStatus.ACCEPTED, profileIdClinician);
        }

        CsvCell patientSeenDateCell = parser.getDatePatientSeen();
        if (!patientSeenDateCell.isEmpty()) {
            Date d = patientSeenDateCell.getDateTime();
            appointmentBuilder.setSentInDateTime(d, patientSeenDateCell);
        }

        CsvCell cancelledDateCell = parser.getDateAppointmentCancelled();
        if (!cancelledDateCell.isEmpty()) {
            Date d = cancelledDateCell.getDateTime();
            appointmentBuilder.setCancelledDateTime(d, cancelledDateCell);
        }

        CsvCell bookingDateCell = parser.getDateAppointmentBooked();
        if (!bookingDateCell.isEmpty()) {
            Date d = bookingDateCell.getDateTime();
            appointmentBuilder.setBookedDateTime(d, bookingDateCell);
        }

        CsvCell telephoneApptCell = parser.getTelephoneAppointment();
        if (telephoneApptCell.getBoolean()) {
            appointmentBuilder.setType("Telephone Appointment", telephoneApptCell);
        }

        CsvCell appointmentStatus = parser.getAppointmentStatus();
        if (!appointmentStatus.isEmpty()) {

            TppMappingRef tppMappingRef = csvHelper.lookUpTppMappingRef(appointmentStatus);
            if (tppMappingRef != null) {
                String statusTerm = tppMappingRef.getMappedTerm();
                Appointment.AppointmentStatus status = convertAppointmentStatus(statusTerm, parser);
                appointmentBuilder.setStatus(status, appointmentStatus);
            } else {
                appointmentBuilder.setStatus(Appointment.AppointmentStatus.PENDING);
            }
        } else {
            appointmentBuilder.setStatus(Appointment.AppointmentStatus.PENDING);
        }

        // Check for appointment flags
        List<AppointmentFlagsPojo> pojoList = csvHelper.getAppointmentFlagCache().getAndRemoveFlagsForAppointmentId(appointmentIdCell.getLong());
        if (pojoList != null) {
            AppointmentFlagCache.applyFlagsToAppointment(csvHelper, appointmentBuilder, pojoList);
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), appointmentBuilder, slotBuilder);
    }

    private static Appointment.AppointmentStatus convertAppointmentStatus(String status, SRAppointment parser) throws Exception {

        if (status.toLowerCase().startsWith("did not attend")
        || status.toLowerCase().trim().contains("no access visit")) {
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
        } else if (status.toLowerCase().startsWith("patient walked out")) {
            return Appointment.AppointmentStatus.CANCELLED;
        } else if (status.toLowerCase().startsWith("rejected")) {
            return Appointment.AppointmentStatus.CANCELLED;
        } else {
            TransformWarnings.log(LOG, parser, "Unrecognized appointment status {} line {} file {}",
                    status, parser.getRowIdentifier().getString(), parser.getFilePath());
            return Appointment.AppointmentStatus.PENDING;
        }
    }


}
