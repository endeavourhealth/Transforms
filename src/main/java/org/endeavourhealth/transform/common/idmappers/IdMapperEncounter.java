package org.endeavourhealth.transform.common.idmappers;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.hl7.fhir.instance.model.Encounter;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

public class IdMapperEncounter extends BaseIdMapper {
    private static final Logger LOG = LoggerFactory.getLogger(IdMapperEncounter.class);

    @Override
    public void getResourceReferences(Resource resource, Set<String> referenceValues) throws Exception {
        Encounter encounter = (Encounter)resource;
        super.addCommonResourceReferences(encounter, referenceValues);

        if (encounter.hasIdentifier()) {
            super.addIndentifierReferences(encounter.getIdentifier(), referenceValues);
        }
        if (encounter.hasPatient()) {
            super.addReference(encounter.getPatient(), referenceValues);
        }
        if (encounter.hasEpisodeOfCare()) {
            super.addReferences(encounter.getEpisodeOfCare(), referenceValues);
        }
        if (encounter.hasIncomingReferral()) {
            super.addReferences(encounter.getIncomingReferral(), referenceValues);
        }
        if (encounter.hasParticipant()) {
            for (Encounter.EncounterParticipantComponent participant: encounter.getParticipant()) {
                if (participant.hasIndividual()) {
                    super.addReference(participant.getIndividual(), referenceValues);
                }
            }
        }
        if (encounter.hasAppointment()) {
            super.addReference(encounter.getAppointment(), referenceValues);
        }
        if (encounter.hasIndication()) {
            super.addReferences(encounter.getIndication(), referenceValues);
        }
        if (encounter.hasLocation()) {
            for (Encounter.EncounterLocationComponent location: encounter.getLocation()) {
                if (location.hasLocation()) {
                    super.addReference(location.getLocation(), referenceValues);
                }
            }
        }
        if (encounter.hasServiceProvider()) {
            super.addReference(encounter.getServiceProvider(), referenceValues);
        }
//        if (encounter.hasPartOf()) {
//            super.addReference(encounter.getPartOf(), referenceValues);
//        }
    }

    @Override
    public void applyReferenceMappings(Resource resource, Map<String, String> mappings, boolean failForMissingMappings) throws Exception {
        Encounter encounter = (Encounter)resource;
        super.mapCommonResourceFields(encounter, mappings, failForMissingMappings);

        if (encounter.hasIdentifier()) {
            super.mapIdentifiers(encounter.getIdentifier(), mappings, failForMissingMappings);
        }
        if (encounter.hasPatient()) {
            super.mapReference(encounter.getPatient(), mappings, failForMissingMappings);
        }
        if (encounter.hasEpisodeOfCare()) {
            super.mapReferences(encounter.getEpisodeOfCare(), mappings, failForMissingMappings);
        }
        if (encounter.hasIncomingReferral()) {
            super.mapReferences(encounter.getIncomingReferral(), mappings, failForMissingMappings);
        }
        if (encounter.hasParticipant()) {
            for (Encounter.EncounterParticipantComponent participant: encounter.getParticipant()) {
                if (participant.hasIndividual()) {
                    super.mapReference(participant.getIndividual(), mappings, failForMissingMappings);
                }
            }
        }
        if (encounter.hasAppointment()) {
            super.mapReference(encounter.getAppointment(), mappings, failForMissingMappings);
        }
        if (encounter.hasIndication()) {
            super.mapReferences(encounter.getIndication(), mappings, failForMissingMappings);
        }
        if (encounter.hasLocation()) {
            for (Encounter.EncounterLocationComponent location: encounter.getLocation()) {
                if (location.hasLocation()) {
                    super.mapReference(location.getLocation(), mappings, failForMissingMappings);
                }
            }
        }
        if (encounter.hasServiceProvider()) {
            super.mapReference(encounter.getServiceProvider(), mappings, failForMissingMappings);
        }
//        if (encounter.hasPartOf()) {
//            super.mapReference(encounter.getPartOf(), mappings, failForMissingMappings);
//        }

    }

    @Override
    public String getPatientId(Resource resource) throws PatientResourceException {

        Encounter encounter = (Encounter)resource;
        if (encounter.hasPatient()) {
            return ReferenceHelper.getReferenceId(encounter.getPatient(), ResourceType.Patient);
        }
        return null;
    }

    /*@Override
    public boolean mapIds(Resource resource, UUID serviceId, UUID systemId, boolean mapResourceId) throws Exception {
        Encounter encounter = (Encounter)resource;

        if (encounter.hasIdentifier()) {
            super.mapIdentifiers(encounter.getIdentifier(), serviceId, systemId);
        }
        if (encounter.hasPatient()) {
            super.mapReference(encounter.getPatient(), serviceId, systemId);
        }
        if (encounter.hasEpisodeOfCare()) {
            super.mapReferences(encounter.getEpisodeOfCare(), serviceId, systemId);
        }
        if (encounter.hasIncomingReferral()) {
            super.mapReferences(encounter.getIncomingReferral(), serviceId, systemId);
        }
        if (encounter.hasParticipant()) {
            for (Encounter.EncounterParticipantComponent participant: encounter.getParticipant()) {
                if (participant.hasIndividual()) {
                    super.mapReference(participant.getIndividual(), serviceId, systemId);
                }
            }
        }
        if (encounter.hasAppointment()) {
            super.mapReference(encounter.getAppointment(), serviceId, systemId);
        }
        if (encounter.hasIndication()) {
            super.mapReferences(encounter.getIndication(), serviceId, systemId);
        }
        if (encounter.hasLocation()) {
            for (Encounter.EncounterLocationComponent location: encounter.getLocation()) {
                if (location.hasLocation()) {
                    super.mapReference(location.getLocation(), serviceId, systemId);
                }
            }
        }
        if (encounter.hasServiceProvider()) {
            super.mapReference(encounter.getServiceProvider(), serviceId, systemId);
        }

        return super.mapCommonResourceFields(encounter, serviceId, systemId, mapResourceId);
    }

    @Override
    public void remapIds(Resource resource, Map<String, String> idMappings) throws Exception {
        Encounter encounter = (Encounter)resource;
        //LOG.debug("Remapping ID for Encounter " + encounter.getId() + " from ID map sized " + idMappings.size());

        if (encounter.hasIdentifier()) {
            super.remapIdentifiers(encounter.getIdentifier(), idMappings);
        }
        if (encounter.hasPatient()) {
            super.remapReference(encounter.getPatient(), idMappings);
        }
        if (encounter.hasEpisodeOfCare()) {
            super.remapReferences(encounter.getEpisodeOfCare(), idMappings);
        }
        if (encounter.hasIncomingReferral()) {
            super.remapReferences(encounter.getIncomingReferral(), idMappings);
        }
        if (encounter.hasParticipant()) {
            for (Encounter.EncounterParticipantComponent participant: encounter.getParticipant()) {
                if (participant.hasIndividual()) {
                    super.remapReference(participant.getIndividual(), idMappings);
                }
            }
        }
        if (encounter.hasAppointment()) {
            super.remapReference(encounter.getAppointment(), idMappings);
        }
        if (encounter.hasIndication()) {
            super.remapReferences(encounter.getIndication(), idMappings);
        }
        if (encounter.hasLocation()) {
            for (Encounter.EncounterLocationComponent location: encounter.getLocation()) {
                if (location.hasLocation()) {
                    super.remapReference(location.getLocation(), idMappings);
                }
            }
        }
        if (encounter.hasServiceProvider()) {
            super.remapReference(encounter.getServiceProvider(), idMappings);
        }

        super.remapCommonResourceFields(encounter, idMappings);
        //LOG.debug("Encounter ID now " + encounter.getId());
    }*/
}
