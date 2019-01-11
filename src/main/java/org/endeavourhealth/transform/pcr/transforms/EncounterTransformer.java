package org.endeavourhealth.transform.pcr.transforms;

import org.endeavourhealth.common.fhir.ExtensionConverter;
import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.core.database.dal.DalProvider;
import org.endeavourhealth.core.database.dal.reference.EncounterCodeDalI;
import org.endeavourhealth.transform.pcr.FhirToPcrCsvTransformer;
import org.endeavourhealth.transform.pcr.PcrTransformParams;
import org.endeavourhealth.transform.pcr.outputModels.AbstractPcrCsvWriter;
import org.hl7.fhir.instance.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class EncounterTransformer extends AbstractTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(EncounterTransformer.class);

    private static final EncounterCodeDalI encounterCodeDal = DalProvider.factoryEncounterCodeDal();

    public boolean shouldAlwaysTransform() {
        return true;
    }

    protected void transformResource(Long pcrId,
                                     Resource resource,
                                     AbstractPcrCsvWriter csvWriter,
                                     PcrTransformParams params) throws Exception {

        Encounter fhir = (Encounter) resource;

        long id = pcrId;
        Long patientId = params.getPcrPatientId();
        Long owningOrganisationId = params.getPcrOrganisationId();
        Date effectiveDate = null;
        Integer effectiveDatePrecision = null;
        Long effectivePractitionerId = null;
        Long enteredByPractitionerId = null;
        Date endDate = null;
        // TODO: 10/01/2019 What's encounterlinkId? Used to link CareEpisodes to Encounter
        String encounterLinkId = null;
        Integer statusConceptId = null;
        Long specialityConceptId = FhirToPcrCsvTransformer.IM_PLACE_HOLDER;
        Long adminConceptId = FhirToPcrCsvTransformer.IM_PLACE_HOLDER;
        Long reasonConceptId = FhirToPcrCsvTransformer.IM_PLACE_HOLDER;
        Integer typeConceptId = null;
        Long locationId = null;
        Long referralRequestId = null;
        Boolean isConsent = false;
        Long latestCareEpisodeStatusId = null;
        Long episodeOfCareId = -1L;


        //recorded/entered by
        Extension enteredByPractitionerExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.RECORDED_BY);
        if (enteredByPractitionerExtension != null) {
            Reference enteredByPractitionerReference = (Reference) enteredByPractitionerExtension.getValue();
            enteredByPractitionerId = transformOnDemandAndMapId(enteredByPractitionerReference, params);
        }
        //consent
        Extension consentExtension = ExtensionConverter.findExtension(fhir, FhirExtensionUri.IS_CONSENT);
        if (consentExtension != null) {
            BooleanType b = (BooleanType) consentExtension.getValue();
            isConsent = b.getValue();
        }
        //CareEpisodes
        if (fhir.hasEpisodeOfCare()) {
            Reference episodeReference = fhir.getEpisodeOfCare().get(0);
            episodeOfCareId = findPcrId(params, episodeReference);
        }
//        if (fhir.hasEpisodeOfCare() && !fhir.getEpisodeOfCare().isEmpty()) {
//            encounterLinkId = resource.getId();
//            List<Reference> references = fhir.getEpisodeOfCare();
//            for (Reference reference : references) {
//                EpisodeOfCare episode = (EpisodeOfCare) findResource(reference, params);
//                if (episode.hasCareManager() && !episode.getCareManager().isEmpty()) {
//                    effectivePractitionerId = transformOnDemandAndMapId(episode.getCareManager(), params);
//                }
//                Long ceId = findOrCreatePcrId(params, ResourceType.EpisodeOfCare.toString(), fhir.getId());
//                org.endeavourhealth.transform.pcr.outputModels.CareEpisode model =
//                        (org.endeavourhealth.transform.pcr.outputModels.CareEpisode) csvWriter;
//                model.writeUpsert(ceId, patientId, owningOrganisationId, effectiveDate, effectiveDatePrecision, effectivePractitionerId, enteredByPractitionerId, endDate,
//                        encounterLinkId, statusConceptId, specialityConceptId, adminConceptId, reasonConceptId, typeConceptId, locationId, referralRequestId, isConsent, latestCareEpisodeStatusId);
//                if (episode.hasCareTeam() && !episode.getCareTeam().isEmpty()) {
//                    for (EpisodeOfCare.EpisodeOfCareCareTeamComponent c : episode.getCareTeam()) {
//                        Resource res = c.getMemberTarget();
//                        if (res.getResourceType().equals(ResourceType.Practitioner)) {
//                            Long practitionerId = findOrCreatePcrId(params, ResourceType.Practitioner.toString(), res.getId());
//                            org.endeavourhealth.transform.pcr.outputModels.CareEpisodeAdditionalPractitioner additionalPractionerModel =
//                                    (org.endeavourhealth.transform.pcr.outputModels.CareEpisodeAdditionalPractitioner) csvWriter;
//                            additionalPractionerModel.writeUpsert(patientId, ceId, practitionerId, enteredByPractitionerId);
//                        }
//                    }
//                }
//                if (episode.hasStatus() && !(episode.getStatus() == null)) ;
//                EpisodeOfCare.EpisodeOfCareStatus episodeOfCareStatus = episode.getStatus();
//                Date startTime = null;
//                Date endTime = null;
//                if (episode.hasPeriod()) {
//                    startTime = episode.getPeriod().getStart();
//                    endTime = episode.getPeriod().getEnd();
//                }
//                String careEpisodeStatus = episodeOfCareStatus.toCode();
//                //TODO how to get statId from status
//                Long careEpisodeStatusId = FhirToPcrCsvTransformer.IM_PLACE_HOLDER;
//                org.endeavourhealth.transform.pcr.outputModels.CareEpisodeStatus episodeStatus =
//                        (org.endeavourhealth.transform.pcr.outputModels.CareEpisodeStatus) csvWriter;
//                episodeStatus.writeUpsert(patientId, owningOrganisationId, enteredByPractitionerId, ceId, startTime, endTime, careEpisodeStatusId);
//            }
//
//
//        }

//TODO now not sure what Encounter maps to
        org.endeavourhealth.transform.pcr.outputModels.Consultation model =
                (org.endeavourhealth.transform.pcr.outputModels.Consultation) csvWriter;
        // model.writeUpsert(id, appointmentScheduleId, slotStart, slotEnd, plannedDurationMinutes, typeConceptId, interactionConceptId);
    }

}
