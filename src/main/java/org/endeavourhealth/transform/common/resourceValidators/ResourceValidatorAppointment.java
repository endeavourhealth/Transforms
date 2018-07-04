package org.endeavourhealth.transform.common.resourceValidators;

import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.hl7.fhir.instance.model.Appointment;
import org.hl7.fhir.instance.model.Duration;
import org.hl7.fhir.instance.model.Resource;

import java.util.List;

public class ResourceValidatorAppointment extends ResourceValidatorBase {

    @Override
    protected void validateResourceFields(Resource resource, List<String> validationErrors) {

        Appointment appointment = (Appointment)resource;
        if (appointment.hasSlot()
                && appointment.getSlot().size() > 1) {
            validationErrors.add("FHIR->Enterprise transform only supports Appointments with a single slot");
        }

        Duration waitDuration = (Duration)ExtensionConverter.findExtensionValue(appointment, FhirExtensionUri.APPOINTMENT_PATIENT_WAIT);
        if (waitDuration != null) {
            if (!waitDuration.getUnit().equalsIgnoreCase("minutes")) {
                validationErrors.add("FHIR->Enterprise transform only supports wait duration in minutes");
            }
        }

        Duration delayDuration = (Duration)ExtensionConverter.findExtensionValue(appointment, FhirExtensionUri.APPOINTMENT_PATIENT_DELAY);
        if (delayDuration != null) {
            if (!delayDuration.getUnit().equalsIgnoreCase("minutes")) {
                validationErrors.add("FHIR->Enterprise transform only supports delay duration in minutes");
            }
        }

    }
}
