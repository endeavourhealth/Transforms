package org.endeavourhealth.transform.tpp.csv.transforms.appointment;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.publisherTransform.models.InternalIdMap;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppMappingRef;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.TransformWarnings;
import org.endeavourhealth.transform.common.resourceBuilders.AppointmentBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.SlotBuilder;
import org.endeavourhealth.transform.tpp.TppCsvHelper;
import org.endeavourhealth.transform.tpp.cache.AppointmentFlagCache;
import org.endeavourhealth.transform.tpp.cache.AppointmentResourceCache;
import org.endeavourhealth.transform.tpp.csv.schema.appointment.SRAppointment;
import org.hl7.fhir.instance.model.Appointment;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class SRAppointmentTransformer {

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
    }

    private static void createResource(SRAppointment parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper) throws Exception {

        CsvCell appointmentId = parser.getRowIdentifier();
        CsvCell patientId = parser.getIDPatient();
        CsvCell deleteData = parser.getRemovedData();
//        boolean mappingNeeded = false;

        if (patientId.isEmpty()) {

            if ((deleteData != null) && !deleteData.isEmpty()) {
                if (!deleteData.getIntAsBoolean()) {
                    TransformWarnings.log(LOG, parser, "No Patient id in record for row: {},  file: {}",
                            parser.getRowIdentifier().getString(), parser.getFilePath());
                    return;
                } else {

                    // get previously filed resources for deletion
                    org.hl7.fhir.instance.model.Appointment appointment
                            = (org.hl7.fhir.instance.model.Appointment) csvHelper.retrieveResource(appointmentId.getString(),
                            ResourceType.Appointment,
                            fhirResourceFiler);

                    org.hl7.fhir.instance.model.Slot slot
                            = (org.hl7.fhir.instance.model.Slot) csvHelper.retrieveResource(appointmentId.getString(),
                            ResourceType.Slot,
                            fhirResourceFiler);

                    if (appointment != null && slot != null) {
                        AppointmentBuilder appointmentBuilder = new AppointmentBuilder(appointment);
                        SlotBuilder slotBuilder = new SlotBuilder(slot);
                        fhirResourceFiler.deletePatientResource(parser.getCurrentState(), appointmentBuilder, slotBuilder);
                        return;
                    }
                }
                return;
            }
        }

        // If we don't have a patient reference, don't file the slot as the filer doesn't support saving slots without a patient
        Reference patientReference = csvHelper.createPatientReference(patientId);
        if (patientReference.isEmpty()) {
            TransformWarnings.log(LOG, parser, "Patient reference not found for row: {},  file: {}",
                    parser.getRowIdentifier().getString(), parser.getFilePath());
            return;
        }

//        if (csvHelper.retrieveResource(patientId.getString(), ResourceType.Patient, fhirResourceFiler) == null) {
//            mappingNeeded = true;
//        }

        //use the same Id reference for the Appointment and the Slot; since it's a different resource type, it should be fine
        AppointmentBuilder appointmentBuilder
                = AppointmentResourceCache.getAppointmentBuilder(appointmentId, csvHelper, fhirResourceFiler);

        appointmentBuilder.addParticipant(patientReference, Appointment.ParticipationStatus.ACCEPTED, patientId);
        SlotBuilder slotBuilder = new SlotBuilder();
        slotBuilder.setId(appointmentId.getString(), appointmentId);

        Reference slotRef = csvHelper.createSlotReference(appointmentId);
        appointmentBuilder.addSlot(slotRef, appointmentId);


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

        CsvCell appointmentStaffProfileId = parser.getIDProfileClinician();
        if (!appointmentStaffProfileId.isEmpty()) {

            String staffMemberId = csvHelper.getInternalId(InternalIdMap.TYPE_TPP_STAFF_PROFILE_ID_TO_STAFF_MEMBER_ID,
                    appointmentStaffProfileId.getString());
            if (!Strings.isNullOrEmpty(staffMemberId)) {
                Reference practitionerReference
                        = ReferenceHelper.createReference(ResourceType.Practitioner, staffMemberId);
                appointmentBuilder.addParticipant(practitionerReference, Appointment.ParticipationStatus.ACCEPTED, appointmentStaffProfileId);
            }
        }

        CsvCell patientSeenDate = parser.getDatePatientSeen();
        if (!patientSeenDate.isEmpty()) {

            Date seenDateTime = DATETIME_FORMAT.parse(patientSeenDate.getString());
            appointmentBuilder.setSentInDateTime(seenDateTime, patientSeenDate);
        }

        CsvCell appointmentStatus = parser.getAppointmentStatus();
        if (!appointmentStatus.isEmpty()) {

            TppMappingRef tppMappingRef = csvHelper.lookUpTppMappingRef(appointmentStatus, parser);
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

        if (AppointmentFlagCache.containsAppointmentId(appointmentId.getLong())) {
            List<AppointmentFlagsPojo> pojoList = AppointmentFlagCache.getStaffMemberProfilePojo(appointmentId.getLong());

            for (AppointmentFlagsPojo pojo : pojoList) {
                TppMappingRef tppMappingRef = csvHelper.lookUpTppMappingRef(pojo.getFlag(), parser);
                if (tppMappingRef != null) {
                    String flagMapping = tppMappingRef.getMappedTerm();
                    if (!Strings.isNullOrEmpty(flagMapping)) {
                        appointmentBuilder.setComments(flagMapping);
                    }
                }
            }
            AppointmentFlagCache.removeFlagsByAppointmentId(appointmentId.getLong());
        }
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), slotBuilder, appointmentBuilder);


    }

    private static Appointment.AppointmentStatus convertAppointmentStatus(String status, SRAppointment parser) throws Exception {

        if (status.toLowerCase().startsWith("did not attend")) {
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
