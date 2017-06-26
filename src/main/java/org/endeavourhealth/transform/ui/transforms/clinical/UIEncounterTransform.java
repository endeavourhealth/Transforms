package org.endeavourhealth.transform.ui.transforms.clinical;

import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.utility.StreamExtension;
import org.endeavourhealth.transform.ui.helpers.CodeHelper;
import org.endeavourhealth.transform.ui.helpers.ExtensionHelper;
import org.endeavourhealth.transform.ui.helpers.PeriodHelper;
import org.endeavourhealth.transform.ui.helpers.ReferencedResources;
import org.endeavourhealth.transform.ui.models.resources.admin.UILocation;
import org.endeavourhealth.transform.ui.models.resources.admin.UIOrganisation;
import org.endeavourhealth.transform.ui.models.resources.admin.UIPractitioner;
import org.endeavourhealth.transform.ui.models.resources.clinicial.UIEncounter;
import org.endeavourhealth.transform.ui.models.types.UICodeableConcept;
import org.endeavourhealth.transform.ui.models.types.UIPeriod;
import org.hl7.fhir.instance.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class UIEncounterTransform extends UIClinicalTransform<Encounter, UIEncounter> {

    public List<UIEncounter> transform(List<Encounter> encounters, ReferencedResources referencedResources) {
        return encounters
                .stream()
                .map(t -> transform(t, referencedResources))
                .collect(Collectors.toList());
    }

    private static UIEncounter transform(Encounter encounter, ReferencedResources referencedResources) {

        return new UIEncounter()
                .setId(encounter.getId())
                .setStatus(getStatus(encounter))
								.setClass_(getClass(encounter))
								.setTypes(getTypes(encounter))
                .setPerformedBy(getPerformedBy(encounter, referencedResources))
                .setRecordedBy(getRecordedBy(encounter, referencedResources))
								.setReferredBy(getReferredBy(encounter, referencedResources))
                .setPeriod(getPeriod(encounter.getPeriod()))
                .setServiceProvider(getServiceProvider(encounter, referencedResources))
                .setRecordedDate(getRecordedDateExtensionValue(encounter))
                .setEncounterSource(getEncounterSource(encounter))
                .setReason(getEncounterReasons(encounter))
								.setLocation(getActiveLocation(encounter, referencedResources))
								.setMessageType(getMessageType(encounter))
								.setEpisodeOfCare(getEpisodeOfCare(encounter.getEpisodeOfCare()))
								.setAdmitted(getAdmittedDate(encounter.getStatusHistory()))
								.setDischarged(getDischargedDate(encounter.getStatusHistory()))
								.setDischargeLocation(getDischargeLocation(encounter.getHospitalization(), referencedResources))
								.setDischargeDisposition(getDischargeDisposition(encounter.getHospitalization()));
    }

	private static UILocation getDischargeLocation(Encounter.EncounterHospitalizationComponent hospitalization, ReferencedResources referencedResources) {
		if (hospitalization == null || hospitalization.getDestination() == null)
			return null;

		return referencedResources.getUILocation(hospitalization.getDestination());
	}

	private static UICodeableConcept getDischargeDisposition(Encounter.EncounterHospitalizationComponent hospitalization) {
		if (hospitalization == null || hospitalization.getDischargeDisposition() == null)
			return null;

		return CodeHelper.convert(hospitalization.getDischargeDisposition());
	}

	private static UIPeriod getAdmittedDate(List<Encounter.EncounterStatusHistoryComponent> statusHistory) {
		Optional<Encounter.EncounterStatusHistoryComponent> admission = statusHistory.stream()
				.filter(sh -> sh.hasStatus() && sh.getStatus() == Encounter.EncounterState.ARRIVED)
				.findFirst();

		if (admission.isPresent())
			return PeriodHelper.convert(admission.get().getPeriod());

		return null;
	}

	private static UIPeriod getDischargedDate(List<Encounter.EncounterStatusHistoryComponent> statusHistory) {
		Optional<Encounter.EncounterStatusHistoryComponent> discharge = statusHistory.stream()
				.filter(sh -> sh.hasStatus() && sh.getStatus() == Encounter.EncounterState.FINISHED)
				.findFirst();

		if (discharge.isPresent())
			return PeriodHelper.convert(discharge.get().getPeriod());

		return null;
	}


	private static UICodeableConcept getEncounterSource(Encounter encounter) {
        CodeableConcept encounterSource = ExtensionHelper.getExtensionValue(encounter, FhirExtensionUri.ENCOUNTER_SOURCE, CodeableConcept.class);
        return CodeHelper.convert(encounterSource);
    }

    private static UIOrganisation getServiceProvider(Encounter encounter, ReferencedResources referencedResources) {
        if (!encounter.hasServiceProvider())
            return null;

        return referencedResources.getUIOrganisation(encounter.getServiceProvider());
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

    private static UIPractitioner getPerformedBy(Encounter encounter, ReferencedResources referencedResources) {
			for (Encounter.EncounterParticipantComponent component : encounter.getParticipant())
				if (component.getType().size() > 0)
					if (component.getType().get(0).getCoding().size() > 0)
						if (component.getType().get(0).getCoding().get(0).getCode().equals("PPRF"))
							return referencedResources.getUIPractitioner(component.getIndividual());

			// If no primary performer, look for attender
			for (Encounter.EncounterParticipantComponent component : encounter.getParticipant())
				if (component.getType().size() > 0)
					if (component.getType().get(0).getCoding().size() > 0)
						if (component.getType().get(0).getCoding().get(0).getCode().equals("ATND"))
							return referencedResources.getUIPractitioner(component.getIndividual());

			return null;
		}

	private static UIPractitioner getReferredBy(Encounter encounter, ReferencedResources referencedResources) {
		for (Encounter.EncounterParticipantComponent component : encounter.getParticipant())
			if (component.getType().size() > 0)
				if (component.getType().get(0).getCoding().size() > 0)
					if (component.getType().get(0).getCoding().get(0).getCode().equals("REF"))
						return referencedResources.getUIPractitioner(component.getIndividual());

		return null;
	}

    private static UIPractitioner getRecordedBy(Encounter encounter, ReferencedResources referencedResources) {
        for (Encounter.EncounterParticipantComponent component : encounter.getParticipant())
            if (component.getType().size() > 0)
                if (component.getType().get(0).getText() == ("Entered by"))
                    return referencedResources.getUIPractitioner(component.getIndividual());

        return null;
    }

    private static List<UICodeableConcept> getEncounterReasons(Encounter encounter) {
        List<UICodeableConcept> reasons = new ArrayList<>();

        for(CodeableConcept reason : encounter.getReason()) {
            reasons.add(CodeHelper.convert(reason));
        }

        return reasons;
    }

    private static UILocation getActiveLocation(Encounter encounter, ReferencedResources referencedResources) {

    	for(Encounter.EncounterLocationComponent location : encounter.getLocation()) {
    		if (location.hasLocation() && location.hasStatus() && location.getStatus() == Encounter.EncounterLocationStatus.ACTIVE)
    			return referencedResources.getUILocation(location.getLocation());
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
                        .filter(t -> t.hasParticipant())
                        .flatMap(t -> t.getParticipant().stream())
                        .filter(t -> t.hasIndividual())
                        .map(t -> t.getIndividual()),
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
