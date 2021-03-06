package org.endeavourhealth.transform.common.resourceBuilders;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
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
        this(appointment, null);
    }

    public AppointmentBuilder(Appointment appointment, ResourceFieldMappingAudit audit) {
        super(audit);

        this.appointment = appointment;
        if (this.appointment == null) {
            this.appointment = new Appointment();
            this.appointment.setStatus(Appointment.AppointmentStatus.PENDING);
            this.appointment.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_APPOINTMENT));
        }
    }

    @Override
    public DomainResource getResource() {
        return appointment;
    }

    /**
     * used to record the PATIENT and assigned PRACTITIONER
     */
    public void addParticipant(Reference reference, Appointment.ParticipationStatus status, CsvCell... sourceCells) {
        Appointment.AppointmentParticipantComponent fhirParticipant = this.appointment.addParticipant();
        fhirParticipant.setActor(reference);
        fhirParticipant.setStatus(status);

        int index = this.appointment.getParticipant().size() - 1;
        auditValue("participant[" + index + "].actor.reference", sourceCells);
    }

    /**
     * records the PRACTITIONER that booked the appointment (not the assigned clinician)
     */
    public void setRecordedBy(Reference practitionerReference, CsvCell... sourceCells) {
        createOrUpdateRecordedByExtension(practitionerReference, sourceCells);
    }

    public void setStartDateTime(Date startDateTime, CsvCell... sourceCells) {
        this.appointment.setStart(startDateTime);

        auditValue("start", sourceCells);
    }

    public void setEndDateTime(Date endDateTime, CsvCell... sourceCells) {
        this.appointment.setEnd(endDateTime);

        auditValue("end", sourceCells);
    }

    public void setMinutesActualDuration(Integer mins, CsvCell... sourceCells) {
        this.appointment.setMinutesDuration(mins);

        auditValue("minutesDuration", sourceCells);
    }

    public void setComments(String comments, CsvCell... sourceCells) {
        this.appointment.setComment(comments);

        auditValue("comments", sourceCells);
    }

    public void addSlot(Reference slotReference, CsvCell... sourceCells) {
        this.appointment.addSlot(slotReference);

        int index = this.appointment.getSlot().size() - 1;
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

    /*public void setDnaReasonCode(CodeableConcept codeableConcept, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateExtension(this.appointment, FhirExtensionUri.APPOINTMENT_DNA_REASON_CODE, codeableConcept);

        auditCodeableConceptExtension(extension, sourceCells);
    }*/

    public void setSentInDateTime(Date sentInDateTime, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateDateTimeExtension(this.appointment, FhirExtensionUri.APPOINTMENT_SENT_IN, sentInDateTime);

        auditDateTimeExtension(extension, sourceCells);
    }

    public void setLeftDateTime(Date leftDateTime, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateDateTimeExtension(this.appointment, FhirExtensionUri.APPOINTMENT_LEFT, leftDateTime);

        auditDateTimeExtension(extension, sourceCells);
    }

    public void setCancelledDateTime(Date cancelledDateTime, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateDateTimeExtension(this.appointment, FhirExtensionUri.APPOINTMENT_CANCELLATION_DATE, cancelledDateTime);

        auditDateTimeExtension(extension, sourceCells);
    }

    public void setBookedDateTime(Date bookedDateTime, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateDateTimeExtension(this.appointment, FhirExtensionUri.APPOINTMENT_BOOKING_DATE, bookedDateTime);

        auditDateTimeExtension(extension, sourceCells);
    }

    public void setOriginalIdentifier(String originalIdentifier, CsvCell... sourceCells) {
        Extension extension = ExtensionConverter.createOrUpdateStringExtension(this.appointment, FhirExtensionUri.APPOINTMENT_ORIGINAL_IDENTIFIER, originalIdentifier);

        auditIdentifierExtension(extension, sourceCells);
    }


    public void setStatus(Appointment.AppointmentStatus status, CsvCell... sourceCells) {
        this.appointment.setStatus(status);

        auditValue("status", sourceCells);
    }

    /**
     * e.g. telephone appointment, home visit
     */
    public void setType(String type, CsvCell... sourceCells) {
        if (Strings.isNullOrEmpty(type)) {
            this.appointment.setType(null);

        } else {
            CodeableConcept cc = CodeableConceptHelper.createCodeableConcept(type);
            this.appointment.setType(cc);

            auditValue("type.text", sourceCells);
        }

    }


    @Override
    public CodeableConcept createNewCodeableConcept(CodeableConceptBuilder.Tag tag, boolean useExisting) {

        if (tag == CodeableConceptBuilder.Tag.Appointment_Dna_Reason_Code) {

            Extension extension = ExtensionConverter.findOrCreateExtension(this.appointment, FhirExtensionUri.APPOINTMENT_DNA_REASON_CODE);
            CodeableConcept codeableConcept = (CodeableConcept) extension.getValue();
            if (codeableConcept != null) {
                if (useExisting) {
                    return codeableConcept;
                } else {
                    throw new IllegalArgumentException("Trying to add new DNA code to Appointment when it already has one");
                }
            }
            codeableConcept = new CodeableConcept();
            extension.setValue(codeableConcept);
            return codeableConcept;

        } else {
            throw new IllegalArgumentException("Unknown tag [" + tag + "]");
        }
    }

    @Override
    public String getCodeableConceptJsonPath(CodeableConceptBuilder.Tag tag, CodeableConcept codeableConcept) {

        if (tag == CodeableConceptBuilder.Tag.Appointment_Dna_Reason_Code) {

            Extension extension = ExtensionConverter.findExtension(this.appointment, FhirExtensionUri.APPOINTMENT_DNA_REASON_CODE);
            if (extension == null) {
                throw new IllegalArgumentException("Can't call getCodeableConceptJsonPath() before calling getOrCreateCodeableConcept()");
            }

            int index = this.appointment.getExtension().indexOf(extension);
            return "extension[" + index + "].valueCodeableConcept";

        } else {
            throw new IllegalArgumentException("Unknown tag [" + tag + "]");
        }
    }

    @Override
    public void removeCodeableConcept(CodeableConceptBuilder.Tag tag, CodeableConcept codeableConcept) {

        if (tag == CodeableConceptBuilder.Tag.Appointment_Dna_Reason_Code) {
            ExtensionConverter.removeExtension(this.appointment, FhirExtensionUri.APPOINTMENT_DNA_REASON_CODE);

        } else {
            throw new IllegalArgumentException("Unknown tag [" + tag + "]");
        }
    }

}
