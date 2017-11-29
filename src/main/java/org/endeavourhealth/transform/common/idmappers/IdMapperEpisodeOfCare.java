package org.endeavourhealth.transform.common.idmappers;

import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.common.exceptions.PatientResourceException;
import org.hl7.fhir.instance.model.EpisodeOfCare;
import org.hl7.fhir.instance.model.Resource;
import org.hl7.fhir.instance.model.ResourceType;

import java.util.Map;
import java.util.Set;

public class IdMapperEpisodeOfCare extends BaseIdMapper {

    @Override
    public void getResourceReferences(Resource resource, Set<String> referenceValues) throws Exception {
        EpisodeOfCare episodeOfCare = (EpisodeOfCare)resource;
        super.addCommonResourceReferences(episodeOfCare, referenceValues);

        if (episodeOfCare.hasIdentifier()) {
            super.addIndentifierReferences(episodeOfCare.getIdentifier(), referenceValues);
        }
        if (episodeOfCare.hasCondition()) {
            super.addReferences(episodeOfCare.getCondition(), referenceValues);
        }
        if (episodeOfCare.hasPatient()) {
            super.addReference(episodeOfCare.getPatient(), referenceValues);
        }
        if (episodeOfCare.hasManagingOrganization()) {
            super.addReference(episodeOfCare.getManagingOrganization(), referenceValues);
        }
        if (episodeOfCare.hasReferralRequest()) {
            super.addReferences(episodeOfCare.getReferralRequest(), referenceValues);
        }
        if (episodeOfCare.hasCareManager()) {
            super.addReference(episodeOfCare.getCareManager(), referenceValues);
        }
        if (episodeOfCare.hasCareTeam()) {
            for (EpisodeOfCare.EpisodeOfCareCareTeamComponent careTeam: episodeOfCare.getCareTeam()) {
                if (careTeam.hasMember()) {
                    super.addReference(careTeam.getMember(), referenceValues);
                }
            }
        }
    }

    @Override
    public void applyReferenceMappings(Resource resource, Map<String, String> mappings, boolean failForMissingMappings) throws Exception {
        EpisodeOfCare episodeOfCare = (EpisodeOfCare)resource;
        super.mapCommonResourceFields(episodeOfCare, mappings, failForMissingMappings);

        if (episodeOfCare.hasIdentifier()) {
            super.mapIdentifiers(episodeOfCare.getIdentifier(), mappings, failForMissingMappings);
        }
        if (episodeOfCare.hasCondition()) {
            super.mapReferences(episodeOfCare.getCondition(), mappings, failForMissingMappings);
        }
        if (episodeOfCare.hasPatient()) {
            super.mapReference(episodeOfCare.getPatient(), mappings, failForMissingMappings);
        }
        if (episodeOfCare.hasManagingOrganization()) {
            super.mapReference(episodeOfCare.getManagingOrganization(), mappings, failForMissingMappings);
        }
        if (episodeOfCare.hasReferralRequest()) {
            super.mapReferences(episodeOfCare.getReferralRequest(), mappings, failForMissingMappings);
        }
        if (episodeOfCare.hasCareManager()) {
            super.mapReference(episodeOfCare.getCareManager(), mappings, failForMissingMappings);
        }
        if (episodeOfCare.hasCareTeam()) {
            for (EpisodeOfCare.EpisodeOfCareCareTeamComponent careTeam: episodeOfCare.getCareTeam()) {
                if (careTeam.hasMember()) {
                    super.mapReference(careTeam.getMember(), mappings, failForMissingMappings);
                }
            }
        }
    }

    @Override
    public String getPatientId(Resource resource) throws PatientResourceException {

        EpisodeOfCare episodeOfCare = (EpisodeOfCare)resource;
        if (episodeOfCare.hasPatient()) {
            return ReferenceHelper.getReferenceId(episodeOfCare.getPatient(), ResourceType.Patient);
        }
        return null;
    }

    /*@Override
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
    }*/
}
