package org.endeavourhealth.transform.tpp.csv.transforms.appointment;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.database.dal.publisherCommon.models.TppMappingRef;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.resourceBuilders.AppointmentBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.ScheduleBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.SlotBuilder;
import org.endeavourhealth.transform.tpp.csv.helpers.TppCsvHelper;
import org.endeavourhealth.transform.tpp.csv.schema.clinical.SRVisit;
import org.hl7.fhir.instance.model.Appointment;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;
import org.hl7.fhir.instance.model.Slot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;
import java.util.UUID;

public class SRVisitTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(SRVisitTransformer.class);

    public static final String VISIT_ID_PREFIX = "Visit:"; //we can't just use the row ID as it will mix up with appointment row IDs

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 TppCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(SRVisit.class);
        if (parser != null) {
            while (parser.nextRecord()) {

                try {
                    createSchedule((SRVisit) parser, fhirResourceFiler, csvHelper);
                    createAppointment((SRVisit) parser, fhirResourceFiler, csvHelper);
                } catch (Exception ex) {
                    fhirResourceFiler.logTransformRecordError(ex, parser.getCurrentState());
                }
            }
        }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    /**
     * since TPP Visits are being saved as FHIR Appointments, we need to create a FHIR Schedule so that the
     * objects look closer to how home visits look from Emis
     */
    private static void createSchedule(SRVisit parser, FhirResourceFiler fhirResourceFiler, TppCsvHelper csvHelper) throws Exception {
        CsvCell visitIdCell = parser.getRowIdentifier();
        String uniqueId = getUniqueId(visitIdCell);

        ScheduleBuilder scheduleBuilder = new ScheduleBuilder();
        scheduleBuilder.setId(uniqueId, visitIdCell);

        //handle delete
        CsvCell deleteDataCell = parser.getRemovedData();
        if (deleteDataCell != null //need null check as column not always present
                && deleteDataCell.getIntAsBoolean()) {

            scheduleBuilder.setDeletedAudit(deleteDataCell);
            fhirResourceFiler.deleteAdminResource(parser.getCurrentState(), scheduleBuilder);
            return;
        }

        //"textual" visits have a patient ID of -1, so ignore these
        CsvCell patientIdCell = parser.getIDPatient();
        if (TppCsvHelper.isEmptyOrNegative(patientIdCell)) {
            return;
        }

        CsvCell startDateCell = parser.getDateRequested();
        if (!startDateCell.isEmpty()) {
            Date startDate = startDateCell.getDateTime();
            scheduleBuilder.setPlanningHorizonStart(startDate, startDateCell);
        }

        CsvCell visitDurationCell = parser.getDuration();
        Date endDate = calculateEndDateTime(startDateCell, visitDurationCell);
        if (endDate != null) {
            scheduleBuilder.setPlanningHorizonEnd(endDate, startDateCell, visitDurationCell);
        }

        CsvCell profileIdAssignedCell = parser.getIDProfileAssigned();
        Reference assignedReference = csvHelper.createPractitionerReferenceForProfileId(profileIdAssignedCell);
        if (assignedReference != null) {
            scheduleBuilder.addActor(assignedReference, profileIdAssignedCell);
        }

        CsvCell bookedByProfileCell = parser.getIDProfileEnteredBy();
        Reference bookedByReference = csvHelper.createPractitionerReferenceForProfileId(bookedByProfileCell);
        if (bookedByReference != null) {
            scheduleBuilder.setRecordedBy(bookedByReference, bookedByProfileCell);
        }

        CsvCell recordedDateCell = parser.getDateBooked();
        if (!recordedDateCell.isEmpty()) {
            scheduleBuilder.setRecordedDate(recordedDateCell.getDateTime(), recordedDateCell);
        }

        //set these so it's clear what it contains
        scheduleBuilder.setTypeFreeText("Home Visit");
        scheduleBuilder.setLocationType("Patient Home");

        //save the Schedule
        fhirResourceFiler.saveAdminResource(parser.getCurrentState(), scheduleBuilder);
    }

    private static Date calculateEndDateTime(CsvCell startDateCell, CsvCell durationCell) throws Exception {
        if (startDateCell.isEmpty()
            || durationCell.isEmpty()) {
            return null;
        }

        Date startDate = startDateCell.getDateTime();
        Integer durationMins = durationCell.getInt();
        long endMillis = startDate.getTime() + (durationMins.intValue() * 60 * 1000);
        return new Date(endMillis);
    }

    private static String getUniqueId(CsvCell visitIdCell) {
        String uniqueId = VISIT_ID_PREFIX + visitIdCell.getString();
        return uniqueId;
    }

    private static void createAppointment(SRVisit parser,
                                       FhirResourceFiler fhirResourceFiler,
                                       TppCsvHelper csvHelper) throws Exception {

        CsvCell visitIdCell = parser.getRowIdentifier();
        String uniqueId = getUniqueId(visitIdCell);



        //handle delete
        CsvCell deleteDataCell = parser.getRemovedData();
        if (deleteDataCell != null && deleteDataCell.getIntAsBoolean()) {

            //to delete the existing FHIR Appointment, we need the patient reference populated, but the record being processed
            //won't have that set, so we need to retrieve the version off the DB first
            Appointment appointment = (Appointment)csvHelper.retrieveResource(uniqueId, ResourceType.Appointment);
            Slot slot = (Slot)csvHelper.retrieveResource(uniqueId, ResourceType.Slot);

            if (appointment != null && slot != null) {
                AppointmentBuilder appointmentBuilder = new AppointmentBuilder(appointment);
                appointmentBuilder.setDeletedAudit(deleteDataCell);

                SlotBuilder slotBuilder = new SlotBuilder(slot);
                slotBuilder.setDeletedAudit(deleteDataCell);

                fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, appointmentBuilder, slotBuilder);

            } else if (appointment != null) { //for older data, we'll just have the appt and no slot
                AppointmentBuilder appointmentBuilder = new AppointmentBuilder(appointment);
                appointmentBuilder.setDeletedAudit(deleteDataCell);

                fhirResourceFiler.deletePatientResource(parser.getCurrentState(), false, appointmentBuilder);
            }

            return;
        }

        //"textual" visits have a patient ID of -1, so ignore these
        CsvCell patientIdCell = parser.getIDPatient();
        if (TppCsvHelper.isEmptyOrNegative(patientIdCell)) {
            return;
        }

        AppointmentBuilder appointmentBuilder = new AppointmentBuilder();
        appointmentBuilder.setId(uniqueId, visitIdCell);

        SlotBuilder slotBuilder = new SlotBuilder();
        slotBuilder.setId(uniqueId, visitIdCell);

        //always set this
        slotBuilder.setFreeBusyType(Slot.SlotStatus.BUSY);

        //slot to schedule
        Reference scheduleRef = ReferenceHelper.createReference(ResourceType.Schedule, uniqueId); //the same unique ID is used for the FHIR Schedule
        slotBuilder.setSchedule(scheduleRef);

        //link appt to slot
        Reference slotRef = ReferenceHelper.createReference(ResourceType.Slot, uniqueId); //the same unique ID is used for the FHIR Schedule
        appointmentBuilder.addSlot(slotRef);

        //critical that we set this because otherwise we can't tell visits from regular appointments
        appointmentBuilder.setType("Home Visit");

        Reference patientReference = csvHelper.createPatientReference(patientIdCell);
        appointmentBuilder.addParticipant(patientReference, Appointment.ParticipationStatus.ACCEPTED, patientIdCell);

        CsvCell startDateCell = parser.getDateRequested();
        if (!startDateCell.isEmpty()) {
            Date startDate = startDateCell.getDateTime();
            appointmentBuilder.setStartDateTime(startDate, startDateCell);
            slotBuilder.setStartDateTime(startDate, startDateCell);
        }

        CsvCell durationCell = parser.getDuration();
        Date endDate = calculateEndDateTime(startDateCell, durationCell);
        if (endDate != null) {
            appointmentBuilder.setEndDateTime(endDate, startDateCell, durationCell);
            slotBuilder.setEndDateTime(endDate, startDateCell, durationCell);
        }

        CsvCell profileIdAssignedCell = parser.getIDProfileAssigned();
        Reference assignedReference = csvHelper.createPractitionerReferenceForProfileId(profileIdAssignedCell);
        if (assignedReference != null) {
            appointmentBuilder.addParticipant(assignedReference, Appointment.ParticipationStatus.ACCEPTED, profileIdAssignedCell);
        }

        CsvCell visitStatusCell = parser.getCurrentStatus();
        if (visitStatusCell.isEmpty()) {
            throw new Exception("Unexpected empty CurrentStatus cell " + visitStatusCell);
        }
        Appointment.AppointmentStatus status = convertStatus(visitStatusCell, csvHelper, assignedReference);
        appointmentBuilder.setStatus(status, visitStatusCell);

        //SD-299 - follow up details is a reference to the TPP mapping reference file, so needs looking up
        CsvCell followUpDetailsCell = parser.getFollowUpDetails();
        String followUpDetailsStr = convertFollowUp(followUpDetailsCell, csvHelper);
        if (!Strings.isNullOrEmpty(followUpDetailsStr)) {
            appointmentBuilder.setComments(followUpDetailsStr, followUpDetailsCell);
        }

        //SD-299 - we weren't doing anything with the date the visit was actually booked (not booked FOR)
        CsvCell recordedDateCell = parser.getDateBooked();
        if (!recordedDateCell.isEmpty()) {
            appointmentBuilder.setBookedDateTime(recordedDateCell.getDateTime(), recordedDateCell);
        }

        //SD-299 - we weren't doing anything with the profile ID of who booked the visit
        CsvCell bookedByProfileCell = parser.getIDProfileEnteredBy();
        Reference bookedByReference = csvHelper.createPractitionerReferenceForProfileId(bookedByProfileCell);
        if (bookedByReference != null) {
            appointmentBuilder.setRecordedBy(bookedByReference, bookedByProfileCell);
        }

        //save both resources
        fhirResourceFiler.savePatientResource(parser.getCurrentState(), appointmentBuilder, slotBuilder);
    }

    private static String convertFollowUp(CsvCell followUpDetailsCell, TppCsvHelper csvHelper) throws Exception {

        if (TppCsvHelper.isEmptyOrNegative(followUpDetailsCell)) {
            return null;
        }

        TppMappingRef tppMappingRef = csvHelper.lookUpTppMappingRef(followUpDetailsCell);
        String refStr = tppMappingRef.getMappedTerm();

        //full list got from: select row_id, mapped_term from publisher_common.tpp_mapping_ref_2 where group_id = 4017;
        if (refStr.equalsIgnoreCase("Unknown")) {
            return null;

        } else if (refStr.equalsIgnoreCase("No")) {
            return "No follow-up visit required";

        } else if (refStr.equalsIgnoreCase("Required")) {
            return "Follow-up visit required";

        } else if (refStr.equalsIgnoreCase("Booked")) {
            return "Follow-up visit booked";

        } else {
            throw new Exception("Unexpected TPP visit follow-up [" + refStr + "] in cell " + followUpDetailsCell);
        }
    }

    private static Appointment.AppointmentStatus convertStatus(CsvCell statusCell, TppCsvHelper csvHelper, Reference practitionerAssignedReference) throws Exception {

        TppMappingRef tppMappingRef = csvHelper.lookUpTppMappingRef(statusCell);
        String status = tppMappingRef.getMappedTerm();

        //full list got from: select row_id, mapped_term from publisher_common.tpp_mapping_ref_2 where group_id = 2005;
        if (status.equalsIgnoreCase("Deleted")) {
            //checked a large sample of raw data and nothing has been received with this status in 2020,
            //so rather than work out how to handle it (i.e. delete FHIR Appointment), just throw an exception if we do ever receive it
            throw new Exception("Unexpected TPP DELETED visit status [" + status + "] in cell " + statusCell);

        } else if (status.equalsIgnoreCase("Finished")) {
            return Appointment.AppointmentStatus.FULFILLED;

        } else if (status.equalsIgnoreCase("Automatically marked as Finished")) {
            return Appointment.AppointmentStatus.FULFILLED;

        } else if (status.equalsIgnoreCase("Requested")) {

            //if there is no practitioner assigned, then count the Visit as just proposed. Only
            //when the visit is assigned ot a staff member count it as actually booked
            if (practitionerAssignedReference == null) {
                return Appointment.AppointmentStatus.PROPOSED;
            } else {
                return Appointment.AppointmentStatus.BOOKED;
            }

        } else if (status.equalsIgnoreCase("Cancelled by Patient")) {
            return Appointment.AppointmentStatus.CANCELLED;

        } else if (status.equalsIgnoreCase("Cancelled by Unit")) {
            return Appointment.AppointmentStatus.CANCELLED;

        } else if (status.equalsIgnoreCase("Cancelled Due to Death")) {
            return Appointment.AppointmentStatus.CANCELLED;

        } else if (status.equalsIgnoreCase("No Access Visit")) {
            return Appointment.AppointmentStatus.NOSHOW;

        } else if (status.equalsIgnoreCase("Deferred")) {
            return Appointment.AppointmentStatus.CANCELLED;

        } else {
            throw new Exception("Unexpected TPP visit status [" + status + "] in cell " + statusCell);
        }

    }
}
