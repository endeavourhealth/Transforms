package org.endeavourhealth.transform.ui.transforms.clinical;

import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.utility.StreamExtension;
import org.endeavourhealth.transform.ui.helpers.*;
import org.endeavourhealth.transform.ui.helpers.ExtensionHelper;
import org.endeavourhealth.transform.ui.models.resources.clinicial.UIEncounter;
import org.endeavourhealth.transform.ui.models.types.UICodeableConcept;
import org.endeavourhealth.transform.ui.models.types.UIDate;
import org.endeavourhealth.transform.ui.models.types.UIInternalIdentifier;
import org.endeavourhealth.transform.ui.models.types.UIPeriod;
import org.hl7.fhir.instance.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

public class UIEncounterTransform extends UIClinicalTransform<Encounter, UIEncounter> {

    public List<UIEncounter> transform(UUID serviceId, UUID systemId, List<Encounter> encounters, ReferencedResources referencedResources) {
        return encounters
                .stream()
                .map(t -> transform(serviceId, systemId, t, referencedResources))
                .collect(Collectors.toList());
    }

    private static UIEncounter transform(UUID serviceId, UUID systemId, Encounter encounter, ReferencedResources referencedResources) {

        return new UIEncounter()
                .setId(encounter.getId())
                .setStatus(getStatus(encounter))
								.setClass_(getClass(encounter))
								.setTypes(getTypes(encounter))
                .setPerformedBy(getPerformedBy(serviceId, systemId, encounter))
                .setRecordedBy(getRecordedBy(serviceId, systemId, encounter))
								.setReferredBy(getReferredBy(serviceId, systemId, encounter))
                .setPeriod(getPeriod(encounter.getPeriod()))
                .setServiceProvider(getServiceProvider(serviceId, systemId, encounter))
                .setRecordedDate(getRecordedDateExtensionValue(encounter))
                .setEncounterSource(getEncounterSource(encounter))
                .setReason(getEncounterReasons(encounter))
								.setLocation(getActiveLocation(serviceId, systemId, encounter))
								.setMessageType(getMessageType(encounter))
								.setEpisodeOfCare(getEpisodeOfCare(encounter.getEpisodeOfCare()))
								.setAdmitted(getAdmittedDate(encounter.getStatusHistory()))
								.setDischarged(getDischargedDate(encounter.getStatusHistory()))
								.setDischargeLocation(getDischargeLocation(serviceId, systemId, encounter.getHospitalization()))
								.setDischargeDisposition(getDischargeDisposition(encounter.getHospitalization()));
    }

	private static UIInternalIdentifier getDischargeLocation(UUID serviceId, UUID systemId, Encounter.EncounterHospitalizationComponent hospitalization) {
		if (hospitalization == null || hospitalization.getDestination() == null || hospitalization.getDestination().getReference() == null)
			return null;

		return new UIInternalIdentifier()
				.setServiceId(serviceId)
				.setSystemId(systemId)
				.setResourceId(UUID.fromString(ReferenceHelper.getReferenceId(hospitalization.getDestination(), ResourceType.Location)));
	}

	private static UICodeableConcept getDischargeDisposition(Encounter.EncounterHospitalizationComponent hospitalization) {
		if (hospitalization == null || hospitalization.getDischargeDisposition() == null)
			return null;

		return CodeHelper.convert(hospitalization.getDischargeDisposition());
	}

	private static UIDate getAdmittedDate(List<Encounter.EncounterStatusHistoryComponent> statusHistory) {
		Optional<Encounter.EncounterStatusHistoryComponent> admission = statusHistory.stream()
				.filter(sh -> sh.hasStatus() && sh.getStatus() == Encounter.EncounterState.ARRIVED)
				.findFirst();

		if (admission.isPresent())
			return DateHelper.convert(admission.get().getPeriod().getStart());

		return null;
	}

	private static UIDate getDischargedDate(List<Encounter.EncounterStatusHistoryComponent> statusHistory) {
		Optional<Encounter.EncounterStatusHistoryComponent> discharge = statusHistory.stream()
				.filter(sh -> sh.hasStatus() && sh.getStatus() == Encounter.EncounterState.FINISHED)
				.findFirst();

		if (discharge.isPresent())
			return DateHelper.convert(discharge.get().getPeriod().getEnd());

		return null;
	}


	private static UICodeableConcept getEncounterSource(Encounter encounter) {
        CodeableConcept encounterSource = ExtensionHelper.getExtensionValue(encounter, FhirExtensionUri.ENCOUNTER_SOURCE, CodeableConcept.class);
        return CodeHelper.convert(encounterSource);
    }

    private static UIInternalIdentifier getServiceProvider(UUID serviceId, UUID systemId, Encounter encounter) {
        if (!encounter.hasServiceProvider())
            return null;

        return new UIInternalIdentifier()
						.setServiceId(serviceId)
						.setSystemId(systemId)
						.setResourceId(UUID.fromString(ReferenceHelper.getReferenceId(encounter.getServiceProvider(), ResourceType.Organization)));

		}

    private static UIPeriod getPeriod(Period period) {
        return new UIPeriod().setStart(period.getStart());
    }

    private static String getStatus(Encounter encounter) {
        if (!encounter.hasStatus())
            return null;

        return encounter.getStatus().toCode();
    }

    private static String getClass(Encounter encounter) {
    	if (!encounter.hasClass_())
    		return null;

    	return encounter.getClass_().toCode();
		}

		private static List<UICodeableConcept> getTypes(Encounter encounter) {
    	if (!encounter.hasType())
    		return null;

			List<UICodeableConcept> types = new ArrayList<>();

			for(CodeableConcept type : encounter.getType()) {
				types.add(CodeHelper.convert(type));
			}

			return types;
		}

    private static UIInternalIdentifier getPerformedBy(UUID serviceId, UUID systemId, Encounter encounter) {
			for (Encounter.EncounterParticipantComponent component : encounter.getParticipant())
				if (component.getType().size() > 0)
					if (component.getType().get(0).getCoding().size() > 0)
						if (component.getType().get(0).getCoding().get(0).getCode().equals("PPRF"))
							return new UIInternalIdentifier()
								.setServiceId(serviceId)
								.setSystemId(systemId)
								.setResourceId(UUID.fromString(ReferenceHelper.getReferenceId(component.getIndividual(), ResourceType.Practitioner)));

			// If no primary performer, look for attender
			for (Encounter.EncounterParticipantComponent component : encounter.getParticipant())
				if (component.getType().size() > 0)
					if (component.getType().get(0).getCoding().size() > 0)
						if (component.getType().get(0).getCoding().get(0).getCode().equals("ATND"))
							return new UIInternalIdentifier()
									.setServiceId(serviceId)
									.setSystemId(systemId)
									.setResourceId(UUID.fromString(ReferenceHelper.getReferenceId(component.getIndividual(), ResourceType.Practitioner)));

			return null;
		}

	private static UIInternalIdentifier getReferredBy(UUID serviceId, UUID systemId, Encounter encounter) {
		for (Encounter.EncounterParticipantComponent component : encounter.getParticipant())
			if (component.getType().size() > 0)
				if (component.getType().get(0).getCoding().size() > 0)
					if (component.getType().get(0).getCoding().get(0).getCode().equals("REF"))
						return new UIInternalIdentifier()
								.setServiceId(serviceId)
								.setSystemId(systemId)
								.setResourceId(UUID.fromString(ReferenceHelper.getReferenceId(component.getIndividual(), ResourceType.Practitioner)));

		return null;
	}

    private static UIInternalIdentifier getRecordedBy(UUID serviceId, UUID systemId, Encounter encounter) {
        for (Encounter.EncounterParticipantComponent component : encounter.getParticipant())
            if (component.getType().size() > 0)
                if (component.getType().get(0).getText() == ("Entered by"))
                    return new UIInternalIdentifier()
												.setServiceId(serviceId)
												.setSystemId(systemId)
												.setResourceId(UUID.fromString(ReferenceHelper.getReferenceId(component.getIndividual(), ResourceType.Practitioner)));

			return null;
    }

    private static List<UICodeableConcept> getEncounterReasons(Encounter encounter) {
        List<UICodeableConcept> reasons = new ArrayList<>();

        for(CodeableConcept reason : encounter.getReason()) {
            reasons.add(CodeHelper.convert(reason));
        }

        return reasons;
    }

    private static UIInternalIdentifier getActiveLocation(UUID serviceId, UUID systemId, Encounter encounter) {

    	for(Encounter.EncounterLocationComponent location : encounter.getLocation()) {
    		if (location.hasLocation() && location.hasStatus() && location.getStatus() == Encounter.EncounterLocationStatus.ACTIVE)
    			return new UIInternalIdentifier()
							.setServiceId(serviceId)
							.setSystemId(systemId)
							.setResourceId(UUID.fromString(ReferenceHelper.getReferenceId(location.getLocation(), ResourceType.Location)));

			}

			return null;
		}

	private static UICodeableConcept getMessageType(Encounter encounter) {
		CodeableConcept encounterSource = ExtensionHelper.getExtensionValue(encounter, FhirExtensionUri.HL7_MESSAGE_TYPE, CodeableConcept.class);
		return CodeHelper.convert(encounterSource);
	}

	private static String getEpisodeOfCare(List<Reference> episodesOfCare) {
    	if (episodesOfCare == null || episodesOfCare.size() == 0)
    		return null;

    	return ReferenceHelper.getReferenceId(episodesOfCare.get(0), ResourceType.EpisodeOfCare);
	}

	public List<Reference> getReferences(List<Encounter> encounters) {
        return StreamExtension.concat(
                encounters
                        .stream()
                        .filter(t -> t.hasPatient())
                        .map(t -> t.getPatient()),
                encounters
                        .stream()
                        .filter(t -> t.hasEpisodeOfCare())
                        .flatMap(t -> t.getEpisodeOfCare().stream()),
                encounters
                        .stream()
                        .filter(t -> t.hasIncomingReferral())
                        .flatMap(t -> t.getIncomingReferral().stream()),
                encounters
                        .stream()
                        .filter(t -> t.hasAppointment())
                        .map(t -> t.getAppointment()),
                encounters
                        .stream()
                        .filter(t -> t.hasLocation())
                        .flatMap(t -> t.getLocation().stream())
                        .map(t -> t.getLocation()),
                encounters
                        .stream()
                        .filter(t -> t.hasServiceProvider())
                        .map(t -> t.getServiceProvider()),
                encounters
                        .stream()
                        .filter(t -> t.hasPartOf())
                        .map(t -> t.getPartOf()))
                .collect(Collectors.toList());
    }
}
