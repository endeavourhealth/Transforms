package org.endeavourhealth.transform.adastra.xml.transforms.clinical;

import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.endeavourhealth.transform.adastra.xml.schema.AdastraCaseDataExport;
import org.endeavourhealth.transform.adastra.AdastraHelper;
import org.endeavourhealth.transform.common.XmlDateHelper;
import org.hl7.fhir.instance.model.Appointment;
import org.hl7.fhir.instance.model.Meta;
import org.hl7.fhir.instance.model.Resource;

import java.util.List;

import static org.endeavourhealth.transform.adastra.AdastraHelper.uniqueIdMapper;

public class AppointmentTransformer {

    public static void transform(AdastraCaseDataExport caseReport, List<Resource> resources) throws Exception {
        AdastraCaseDataExport.LatestAppointment appointment = caseReport.getLatestAppointment();

        Appointment fhirAppointment = new Appointment();
        fhirAppointment.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_APPOINTMENT));

        fhirAppointment.setId(caseReport.getAdastraCaseReference() + ":" + appointment.getAppointmentTime() + ":" + appointment.getLocation());
        uniqueIdMapper.put("latestAppointment", fhirAppointment.getId());

        fhirAppointment.setStart(XmlDateHelper.convertDate(appointment.getAppointmentTime()));
        fhirAppointment.setStatus(getAppointmentStatus(appointment.getStatus()));

        Appointment.AppointmentParticipantComponent fhirParticipant = fhirAppointment.addParticipant();
        fhirParticipant.setActor(AdastraHelper.createLocationReference(appointment.getLocation()));
    }

    private static Appointment.AppointmentStatus getAppointmentStatus(String status) throws Exception {

        switch (status) {
            case "Arrived":
                return Appointment.AppointmentStatus.ARRIVED;
            case "DidNotAttend":
                return Appointment.AppointmentStatus.NOSHOW;
            case "Cancelled":
                return Appointment.AppointmentStatus.CANCELLED;
            case "None":
                return Appointment.AppointmentStatus.NULL;
            default:
                throw new Exception("Unexpected Appointment Status [" + status+ "]");
        }
    }
}
