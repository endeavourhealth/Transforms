package org.endeavourhealth.transform.emis.csv.transforms.appointment;

import org.endeavourhealth.common.fhir.QuantityHelper;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.core.exceptions.CodeNotFoundException;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.IdHelper;
import org.endeavourhealth.transform.common.resourceBuilders.AppointmentBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.SlotBuilder;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCodeHelper;
import org.endeavourhealth.transform.emis.csv.helpers.EmisCsvHelper;
import org.endeavourhealth.transform.emis.csv.schema.appointment.Slot;
import org.hl7.fhir.instance.model.Appointment;
import org.hl7.fhir.instance.model.Duration;
import org.hl7.fhir.instance.model.Reference;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.*;

public class SlotTransformer {

    public static void transform(Map<Class, AbstractCsvParser> parsers,
                                 FhirResourceFiler fhirResourceFiler,
                                 EmisCsvHelper csvHelper) throws Exception {

        AbstractCsvParser parser = parsers.get(Slot.class);
           while (parser != null && parser.nextRecord()) {
                try {
                    createSlotAndAppointment((Slot) parser, fhirResourceFiler, csvHelper);
                } catch (CodeNotFoundException ex) {
                   String errorRecClsName = Thread.currentThread().getStackTrace()[1].getClassName();
                    csvHelper.logErrorRecord(ex, ((Slot) parser).getPatientGuid(), ((Slot) parser).getSlotGuid(),errorRecClsName);
                }
            }

        //call this to abort if we had any errors, during the above processing
        fhirResourceFiler.failIfAnyErrors();
    }

    public static void createSlotAndAppointment(Slot parser,
                                                 FhirResourceFiler fhirResourceFiler,
                                                 EmisCsvHelper csvHelper) throws Exception {

        CsvCell slotGuid = parser.getSlotGuid();
        CsvCell patientGuid = parser.getPatientGuid();


        //the slots CSV contains data on empty slots too; ignore them
        if (patientGuid.isEmpty()) {
            return;
        }

        //the EMIS data contains thousands of appointments that refer to patients we don't have, so I'm explicitly
        //handling this here, and ignoring any Slot record that is in this state
        UUID patientEdsId = IdHelper.getEdsResourceId(fhirResourceFiler.getServiceId(), ResourceType.Patient, patientGuid.getString());
        if (patientEdsId == null) {
            return;
        }


        SlotBuilder slotBuilder = new SlotBuilder();
        AppointmentBuilder appointmentBuilder = new AppointmentBuilder();

        //use the same slot GUID as the appointment GUID; since it's a different resource type, it should be fine
        EmisCsvHelper.setUniqueId(slotBuilder, patientGuid, slotGuid);
        EmisCsvHelper.setUniqueId(appointmentBuilder, patientGuid, slotGuid);

        //moved this higher up, because we need to have set the patient ID on the resource before we delete it
        Reference patientReference = EmisCsvHelper.createPatientReference(patientGuid);
        appointmentBuilder.addParticipant(patientReference, Appointment.ParticipationStatus.ACCEPTED, patientGuid);

        //if the Resource is to be deleted from the data store, then stop processing the CSV row
        CsvCell deletedCell = parser.getDeleted();
        if (deletedCell.getBoolean()) {
            slotBuilder.setDeletedAudit(deletedCell);
            appointmentBuilder.setDeletedAudit(deletedCell);
            fhirResourceFiler.deletePatientResource(parser.getCurrentState(), slotBuilder, appointmentBuilder);
            return;
        }

        CsvCell sessionGuid = parser.getSessionGuid();
        Reference scheduleReference = csvHelper.createScheduleReference(sessionGuid);
        slotBuilder.setSchedule(scheduleReference, sessionGuid);

        //because we're only storing slots with patients, all slots are "busy"
        slotBuilder.setFreeBusyType(org.hl7.fhir.instance.model.Slot.SlotStatus.BUSY);

        CsvCell startDate = parser.getAppointmentStartDate();
        CsvCell startTime = parser.getAppointmentStartTime();
        Date startDateTime = CsvCell.getDateTimeFromTwoCells(startDate, startTime);

        slotBuilder.setStartDateTime(startDateTime, startDate, startTime);
        appointmentBuilder.setStartDateTime(startDateTime, startDate, startTime);

        //calculate expected end datetime from start, plus duration in mins
        CsvCell durationMins = parser.getPlannedDurationInMinutes();
        long endMillis = startDateTime.getTime() + (durationMins.getInt() * 60 * 1000);

        slotBuilder.setEndDateTime(new Date(endMillis), durationMins);
        appointmentBuilder.setEndDateTime(new Date(endMillis), durationMins);

        CsvCell duration = parser.getActualDurationInMinutes();
        if (!duration.isEmpty()) {
            appointmentBuilder.setMinutesDuration(duration.getInt(), duration);
        }

        Reference slotReference = csvHelper.createSlotReference(patientGuid, slotGuid);
        appointmentBuilder.addSlot(slotReference, slotGuid);

        //if we get an update to an appointment, we don't get the practitioners again, so we need to retrieve the existing instance
        List<CsvCell> userGuidCells = retrieveExistingPractitioners(slotGuid, csvHelper, fhirResourceFiler);

        List<CsvCell> newUsersToSave = csvHelper.findSessionPractitionersToSave(sessionGuid);
        CsvCell.addAnyMissingByValue(userGuidCells, newUsersToSave);

        List<CsvCell> newUsersToDelete = csvHelper.findSessionPractitionersToDelete(sessionGuid);
        CsvCell.removeAnyByValue(userGuidCells, newUsersToDelete);

        //apply the users to the FHIR resource
        for (CsvCell userGuidCell : userGuidCells) {
            Reference practitionerReference = ReferenceHelper.createReference(ResourceType.Practitioner, userGuidCell.getString());
            appointmentBuilder.addParticipant(practitionerReference, Appointment.ParticipationStatus.ACCEPTED, userGuidCell);
        }

        CsvCell patientWaitCell = parser.getPatientWaitInMin();
        if (!patientWaitCell.isEmpty()) {
            Duration fhirDuration = QuantityHelper.createDuration(patientWaitCell.getInt(), "minutes");
            appointmentBuilder.setPatientWait(fhirDuration, patientWaitCell);
        }

        CsvCell patientDelayMins = parser.getAppointmentDelayInMin();
        if (!patientDelayMins.isEmpty()) {
            Duration fhirDuration = QuantityHelper.createDuration(patientDelayMins.getInt(), "minutes");
            appointmentBuilder.setPatientDelay(fhirDuration, patientDelayMins);
        }

        CsvCell dnaReasonCode = parser.getDnaReasonCodeId();
        EmisCodeHelper.createCodeableConcept(appointmentBuilder, false, dnaReasonCode, CodeableConceptBuilder.Tag.Appointment_Dna_Reason_Code, csvHelper);

        CsvCell sentInTime = parser.getSendInTime();
        if (!sentInTime.isEmpty()) {
            //combine the sent in TIME with the start DATE
            Date sentInDateTime = CsvCell.getDateTimeFromTwoCells(startDate, sentInTime);
            appointmentBuilder.setSentInDateTime(sentInDateTime, startDate, sentInTime);
        }

        CsvCell leftTime = parser.getLeftTime();
        if (!leftTime.isEmpty()) {
            //combine the left in TIME with the start DATE
            Date leftDateTime = CsvCell.getDateTimeFromTwoCells(startDate, leftTime);
            appointmentBuilder.setLeftDateTime(leftDateTime, startDate, leftTime);
        }

        //derive the status from the other fields - note cancellation isn't worked out here - if an appt
        //is cancelled, then the next extract will simply have the slot without a patientGuid, which is handled
        //in the SlotPreTransformer class
        CsvCell didNotAttend = parser.getDidNotAttend();
        if (didNotAttend.getBoolean()) {
            appointmentBuilder.setStatus(Appointment.AppointmentStatus.NOSHOW, didNotAttend);

        } else if (!leftTime.isEmpty()) {
            appointmentBuilder.setStatus(Appointment.AppointmentStatus.FULFILLED, leftTime);

        } else if (!sentInTime.isEmpty()) {
            appointmentBuilder.setStatus(Appointment.AppointmentStatus.ARRIVED, sentInTime);

        } else {
            //otherwise the appointment is booked
            appointmentBuilder.setStatus(Appointment.AppointmentStatus.BOOKED);
        }

        fhirResourceFiler.savePatientResource(parser.getCurrentState(), slotBuilder, appointmentBuilder);
    }





    private static List<CsvCell> retrieveExistingPractitioners(CsvCell slotGuidCell, EmisCsvHelper csvHelper, FhirResourceFiler fhirResourceFiler) throws Exception {

        List<CsvCell> ret = new ArrayList<>();

        String slotGuid = slotGuidCell.getString();
        Appointment existingAppointment = (Appointment)csvHelper.retrieveResource(slotGuid, ResourceType.Appointment);
        if (existingAppointment != null
                && existingAppointment.hasParticipant()) {

            List<Reference> edsReferences = new ArrayList<>();

            //note the participant list will also have the PATIENT reference in, so skip that
            for (Appointment.AppointmentParticipantComponent participant: existingAppointment.getParticipant()) {
                Reference reference = participant.getActor();
                ResourceType resourceType = ReferenceHelper.getResourceType(reference);
                if (resourceType != ResourceType.Patient) {
                    edsReferences.add(reference);
                }
            }

            //the existing resource will have been through the mapping process, so we need to reverse-lookup the source EMIS user GUIDs from the EDS UUIDs
            List<Reference> rawReferences = IdHelper.convertEdsReferencesToLocallyUniqueReferences(csvHelper, edsReferences);

            for (Reference rawReference : rawReferences) {
                String emisUserGuid = ReferenceHelper.getReferenceId(rawReference);

                //our list expects CsvCells, so create a dummy cell with a row audit of -1, which will automatically be ignored
                CsvCell wrapperCell = CsvCell.factoryDummyWrapper(emisUserGuid);
                ret.add(wrapperCell);
            }
        }

        return ret;
    }

}