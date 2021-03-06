package org.endeavourhealth.transform.common.idmappers;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.hl7.fhir.instance.model.Appointment;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.Map;
import java.util.Set;

public class IdMapperAppointment extends BaseIdMapper {

    @Override
    public void getResourceReferences(Resource resource, Set<String> referenceValues) throws Exception {
        Appointment appointment = (Appointment)resource;
        super.addCommonResourceReferences(appointment, referenceValues);

        if (appointment.hasIdentifier()) {
            super.addIndentifierReferences(appointment.getIdentifier(), referenceValues);
        }
        if (appointment.hasSlot()) {
            super.addReferences(appointment.getSlot(), referenceValues);
        }
        if (appointment.hasParticipant()) {
            for (Appointment.AppointmentParticipantComponent participant: appointment.getParticipant()) {
                if (participant.hasActor()) {
                    super.addReference(participant.getActor(), referenceValues);
                }
            }
        }
    }

    @Override
    public void applyReferenceMappings(Resource resource, Map<String, String> mappings, boolean failForMissingMappings) throws Exception {
        Appointment appointment = (Appointment)resource;

        super.mapCommonResourceFields(appointment, mappings, failForMissingMappings);

        if (appointment.hasIdentifier()) {
            super.mapIdentifiers(appointment.getIdentifier(), mappings, failForMissingMappings);
        }
        if (appointment.hasSlot()) {
            super.mapReferences(appointment.getSlot(), mappings, failForMissingMappings);
        }
        if (appointment.hasParticipant()) {
            for (Appointment.AppointmentParticipantComponent participant: appointment.getParticipant()) {
                if (participant.hasActor()) {
                    super.mapReference(participant.getActor(), mappings, failForMissingMappings);
                }
            }
        }

    }

    @Override
    public String getPatientId(Resource resource) throws PatientResourceException {

        Appointment appointment = (Appointment)resource;
        if (appointment.hasParticipant()) {
            for (Appointment.AppointmentParticipantComponent participant: appointment.getParticipant()) {
                String id = ReferenceHelper.getReferenceId(participant.getActor(), ResourceType.Patient);
                if (id != null) {
                    return id;
                }
            }
        }
        return null;
    }

    /*@Override
    public boolean mapIds(Resource resource, UUID serviceId, UUID systemId, boolean mapResourceId) throws Exception {
        Appointment appointment = (Appointment)resource;

        if (appointment.hasIdentifier()) {
            super.mapIdentifiers(appointment.getIdentifier(), serviceId, systemId);
        }
        if (appointment.hasSlot()) {
            super.mapReferences(appointment.getSlot(), serviceId, systemId);
        }
        if (appointment.hasParticipant()) {
            for (Appointment.AppointmentParticipantComponent participant: appointment.getParticipant()) {
                if (participant.hasActor()) {
                    super.mapReference(participant.getActor(), serviceId, systemId);
                }
            }
        }

        return super.mapCommonResourceFields(appointment, serviceId, systemId, mapResourceId);
    }


    @Override
    public void remapIds(Resource resource, Map<String, String> idMappings) throws Exception {
        throw new Exception("Resource type not supported for remapping");
    }*/
}
