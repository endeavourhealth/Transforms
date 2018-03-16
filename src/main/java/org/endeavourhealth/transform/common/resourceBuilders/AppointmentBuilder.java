package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

import java.util.Date;

public class AppointmentBuilder extends ResourceBuilderBase
                                implements HasCodeableConceptI {

    private Appointment appointment = null;

    public AppointmentBuilder() {
        this(null);
    }

    public AppointmentBuilder(Appointment appointment) {
        this.appointment = appointment;
        if (this.appointment == null) {
            this.appointment = new Appointment();
            this.appointment.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_APPOINTMENT));
        }
    }

    @Override
    public DomainResource getResource() {
        return appointment;
    }

    public void addParticipant(Reference reference, Appointment.ParticipationStatus status, CsvCell... sourceCells) {
        Appointment.AppointmentParticipantComponent fhirParticipant = this.appointment.addParticipant();
        fhirParticipant.setActor(reference);
        fhirParticipant.setStatus(status);

        int index = this.appointment.getParticipant().size()-1;
        auditValue("participant[" + index + "].actor.reference", sourceCells);
    }

    public void setStartDateTime(Date startDateTime, CsvCell... sourceCells) {
        this.appointment.setStart(startDateTime);

        auditValue("start", sourceCells);
    }

    public void setEndDateTime(Date endDateTime, CsvCell... sourceCells) {
        this.appointment.setEnd(endDateTime);

        auditValue("end", sourceCells);
    }

    public void setMinutesDuration(Integer mins, CsvCell... sourceCells) {
        this.appointment.setMinutesDuration(mins);

        auditValue("minutesDuration", sourceCells);
    }

    public void addSlot(Reference slotReference, CsvCell... sourceCells) {
        this.appointment.addSlot(slotReference);

        int index = this.appointment.getSlot().size()-1;
        auditValue("slot[" + index + "].reference", sourceCells);
    }

    public void setPatientWait(Duration duration, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateExtension(this.appointment, FhirExtensionUri.APPOINTMENT_PATIENT_WAIT, duration);

        auditDurationExtension(extension, sourceCells);
    }

    public void setPatientDelay(Duration duration, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateExtension(this.appointment, FhirExtensionUri.APPOINTMENT_PATIENT_DELAY, duration);

        auditDurationExtension(extension, sourceCells);
    }

    public void setDnaReasonCode(CodeableConcept codeableConcept, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateExtension(this.appointment, FhirExtensionUri.APPOINTMENT_DNA_REASON_CODE, codeableConcept);

        auditCodeableConceptExtension(extension, sourceCells);
    }

    public void setSentInDateTime(Date sentInDateTime, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateDateTimeExtension(this.appointment, FhirExtensionUri.APPOINTMENT_SENT_IN, sentInDateTime);

        auditDateTimeExtension(extension, sourceCells);
    }

    public void setLeftDateTime(Date leftDateTime, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateDateTimeExtension(this.appointment, FhirExtensionUri.APPOINTMENT_LEFT, leftDateTime);

        auditDateTimeExtension(extension, sourceCells);
    }

    public void setStatus(Appointment.AppointmentStatus status, CsvCell... sourceCells) {
        this.appointment.setStatus(status);

        auditValue("status", sourceCells);
    }


    @Override
    public CodeableConcept createNewCodeableConcept(String tag) {
        Extension extension = ExtensionConverter.findOrCreateExtension(this.appointment, FhirExtensionUri.APPOINTMENT_DNA_REASON_CODE);
        CodeableConcept codeableConcept = (CodeableConcept)extension.getValue();
        if (codeableConcept != null) {
            throw new IllegalArgumentException("Trying to add new DNA code to Appointment when it already has one");
        }
        codeableConcept = new CodeableConcept();
        extension.setValue(codeableConcept);
        return codeableConcept;
    }

    @Override
    public String getCodeableConceptJsonPath(String tag, CodeableConcept codeableConcept) {
        Extension extension = ExtensionConverter.findExtension(this.appointment, FhirExtensionUri.APPOINTMENT_DNA_REASON_CODE);
        if (extension == null) {
            throw new IllegalArgumentException("Can't call getCodeableConceptJsonPath() before calling getOrCreateCodeableConcept()");
        }

        int index = this.appointment.getExtension().indexOf(extension);
        return "extension[" + index + "].valueCodeableConcept";
    }

    @Override
    public void removeCodeableConcept(String tag, CodeableConcept codeableConcept) {
        ExtensionConverter.removeExtension(this.appointment, FhirExtensionUri.APPOINTMENT_DNA_REASON_CODE);
    }
}
