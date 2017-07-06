package org.endeavourhealth.transform.common.idmappers;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.hl7.fhir.instance.model.EpisodeOfCare;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.Map;
import java.util.UUID;

public class IdMapperEpisodeOfCare extends BaseIdMapper {
    @Override
    public boolean mapIds(Resource resource, UUID serviceId, UUID systemId, boolean mapResourceId) throws Exception {
        EpisodeOfCare episodeOfCare = (EpisodeOfCare)resource;

        if (episodeOfCare.hasIdentifier()) {
            super.mapIdentifiers(episodeOfCare.getIdentifier(), serviceId, systemId);
        }
        if (episodeOfCare.hasCondition()) {
            super.mapReferences(episodeOfCare.getCondition(), serviceId, systemId);
        }
        if (episodeOfCare.hasPatient()) {
            super.mapReference(episodeOfCare.getPatient(), serviceId, systemId);
        }
        if (episodeOfCare.hasManagingOrganization()) {
            super.mapReference(episodeOfCare.getManagingOrganization(), serviceId, systemId);
        }
        if (episodeOfCare.hasReferralRequest()) {
            super.mapReferences(episodeOfCare.getReferralRequest(), serviceId, systemId);
        }
        if (episodeOfCare.hasCareManager()) {
            super.mapReference(episodeOfCare.getCareManager(), serviceId, systemId);
        }
        if (episodeOfCare.hasCareTeam()) {
            for (EpisodeOfCare.EpisodeOfCareCareTeamComponent careTeam: episodeOfCare.getCareTeam()) {
                if (careTeam.hasMember()) {
                    super.mapReference(careTeam.getMember(), serviceId, systemId);
                }
            }
        }

        return super.mapCommonResourceFields(episodeOfCare, serviceId, systemId, mapResourceId);
    }

    @Override
    public String getPatientId(Resource resource) throws PatientResourceException {

        EpisodeOfCare episodeOfCare = (EpisodeOfCare)resource;
        if (episodeOfCare.hasPatient()) {
            return ReferenceHelper.getReferenceId(episodeOfCare.getPatient(), ResourceType.Patient);
        }
        return null;
    }

    @Override
    public void remapIds(Resource resource, Map<String, String> idMappings) throws Exception {
        EpisodeOfCare episodeOfCare = (EpisodeOfCare)resource;

        if (episodeOfCare.hasIdentifier()) {
            super.remapIdentifiers(episodeOfCare.getIdentifier(), idMappings);
        }
        if (episodeOfCare.hasCondition()) {
            super.remapReferences(episodeOfCare.getCondition(), idMappings);
        }
        if (episodeOfCare.hasPatient()) {
            super.remapReference(episodeOfCare.getPatient(), idMappings);
        }
        if (episodeOfCare.hasManagingOrganization()) {
            super.remapReference(episodeOfCare.getManagingOrganization(), idMappings);
        }
        if (episodeOfCare.hasReferralRequest()) {
            super.remapReferences(episodeOfCare.getReferralRequest(), idMappings);
        }
        if (episodeOfCare.hasCareManager()) {
            super.remapReference(episodeOfCare.getCareManager(), idMappings);
        }
        if (episodeOfCare.hasCareTeam()) {
            for (EpisodeOfCare.EpisodeOfCareCareTeamComponent careTeam: episodeOfCare.getCareTeam()) {
                if (careTeam.hasMember()) {
                    super.remapReference(careTeam.getMember(), idMappings);
                }
            }
        }

        super.remapCommonResourceFields(episodeOfCare, idMappings);
    }
}
