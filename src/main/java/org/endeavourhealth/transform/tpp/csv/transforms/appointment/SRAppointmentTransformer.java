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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class SRAppointmentTransformer {

    // FHIR filing note: we create a new slot from constructor, therefore we have to do a
    // mapId on slot, therefore we need to use local ids for appointment as well.

    private static final Logger LOG = LoggerFactory.getLogger(SRAppointmentTransformer.class);

    public static final DateFormat DATETIME_FORMAT = new SimpleDateFormat("dd MMM yyyy HH:mm:ss");

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

        CsvCell appointmentId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();
        CsvCell deleteData = parser.getRemovedData();

        if (deleteData != null && deleteData.getIntAsBoolean()) {

            // get previously filed resources for deletion
            Appointment appointment = (Appointment) csvHelper.retrieveResource(appointmentId.getString(), ResourceType.Appointment);
            if (appointment != null) {
                AppointmentBuilder appointmentBuilder = new AppointmentBuilder(appointment);
                appointmentBuilder.setDeletedAudit(deleteData);
                fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, appointmentBuilder);
            }
            //TODO: Slot is not a Patient resource, confirm what to delete
//            Slot slot = (Slot) csvHelper.retrieveResource(appointmentId.getString(), ResourceType.Slot);
//            if (slot != null) {
//                SlotBuilder slotBuilder = new SlotBuilder(slot);
//                fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, slotBuilder);
//            }
//            return;
        }

        // If we don't have a patient reference, don't file the slot as the filer doesn't support saving slots without a patient
        if (patientId.isEmpty()) {
            return;
        }

        //use the same Id reference for the Appointment and the Slot; since it's a different resource type, it should be fine
        AppointmentBuilder appointmentBuilder = new AppointmentBuilder();
        appointmentBuilder.setId(appointmentId.getString(), appointmentId);

        SlotBuilder slotBuilder = new SlotBuilder();
        slotBuilder.setId(appointmentId.getString(), appointmentId);

        Reference patientReference = csvHelper.createPatientReference(patientId);
        appointmentBuilder.addParticipant(patientReference, Appointment.ParticipationStatus.ACCEPTED, patientId);

        Reference slotRef = csvHelper.createSlotReference(appointmentId);
        appointmentBuilder.addSlot(slotRef, appointmentId);

        CsvCell rotaId = parser.getIDRota();
        if (!rotaId.isEmpty()) {
            Reference scheduleReference = csvHelper.createScheduleReference(rotaId);
            slotBuilder.setSchedule(scheduleReference, rotaId);
        }

        //because we're only storing slots with patients, all slots are "busy"
        slotBuilder.setFreeBusyType(Slot.SlotStatus.BUSY);

        //cell is both date and time, so create datetime from both
        CsvCell startDate = parser.getDateStart();
        CsvCell startTime = parser.getDateStart();
        Date startDateTime = null;
        if (!startDate.isEmpty()) {
            startDateTime = DATETIME_FORMAT.parse(startTime.getString());
            slotBuilder.setStartDateTime(startDateTime, startDate);
            appointmentBuilder.setStartDateTime(startDateTime, startDate);
        }

        CsvCell endDate = parser.getDateEnd();
        Date endDateTime = null;
        if (!endDate.isEmpty()) {
            endDateTime = DATETIME_FORMAT.parse(endDate.getString());
            slotBuilder.setEndDateTime(endDateTime, endDate);
            appointmentBuilder.setEndDateTime(endDateTime, endDate);
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

        CsvCell patientSeenDate = parser.getDatePatientSeen();
        if (!patientSeenDate.isEmpty()) {

            Date seenDateTime = DATETIME_FORMAT.parse(patientSeenDate.getString());
            appointmentBuilder.setSentInDateTime(seenDateTime, patientSeenDate);
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
        List<AppointmentFlagsPojo> pojoList = csvHelper.getAppointmentFlagCache().getAndRemoveFlagsForAppointmentId(appointmentId.getLong());
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
