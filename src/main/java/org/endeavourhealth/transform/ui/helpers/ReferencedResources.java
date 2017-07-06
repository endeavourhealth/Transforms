package org.endeavourhealth.transform.ui.helpers;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.common.utility.StreamExtension;
import org.endeavourhealth.transform.ui.models.resources.admin.UILocation;
import org.endeavourhealth.transform.ui.models.resources.admin.UIOrganisation;
import org.endeavourhealth.transform.ui.models.resources.admin.UIPractitioner;
import org.endeavourhealth.transform.ui.models.resources.clinicial.UIMedication;
import org.endeavourhealth.transform.ui.models.resources.clinicial.UIMedicationStatement;
import org.endeavourhealth.transform.ui.models.resources.clinicial.UIObservation;
import org.endeavourhealth.transform.ui.transforms.admin.UILocationTransform;
import org.endeavourhealth.transform.ui.transforms.admin.UIOrganisationTransform;
import org.endeavourhealth.transform.ui.transforms.admin.UIPractitionerTransform;
import org.endeavourhealth.transform.ui.transforms.clinical.UIMedicationStatementTransform;
import org.endeavourhealth.transform.ui.transforms.clinical.UIMedicationTransform;
import org.endeavourhealth.transform.ui.transforms.clinical.UIObservationTransform;
import org.hl7.fhir.instance.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class ReferencedResources {
	private List<Organization> organisations = new ArrayList<>();
	private List<UIOrganisation> uiOrganisations = new ArrayList<>();
	private List<Location> locations = new ArrayList<>();
	private List<UILocation> uiLocations = new ArrayList<>();
	private List<Medication> medications = new ArrayList<>();
	private List<UIMedication> uiMedications = new ArrayList<>();
	private List<MedicationStatement> medicationStatements = new ArrayList<>();
	private List<UIMedicationStatement> uiMedicationStatements = new ArrayList<>();
	private List<Observation> observations = new ArrayList<>();
	private List<UIObservation> uiObservations = new ArrayList<>();

	public UIOrganisation getUIOrganisation(Reference reference) {
		String referenceId = ReferenceHelper.getReferenceId(reference, ResourceType.Organization);

		if (StringUtils.isEmpty(referenceId))
			return null;

		return this
				.uiOrganisations
				.stream()
				.filter(t -> t.getId().equals(referenceId))
				.collect(StreamExtension.firstOrNullCollector());
	}

	public void setOrganisations(List<Organization> organisations) {
		this.organisations = organisations;

		this.uiOrganisations = organisations
				.stream()
				.map(t -> UIOrganisationTransform.transform(t))
				.collect(Collectors.toList());
	}

	public UILocation getUILocation(Reference reference) {
		String referenceId = ReferenceHelper.getReferenceId(reference, ResourceType.Location);

		if (StringUtils.isEmpty(referenceId))
			return null;

		return this
				.uiLocations
				.stream()
				.filter(t -> t.getId().equals(referenceId))
				.collect(StreamExtension.firstOrNullCollector());
	}

	public void setLocations(List<Location> locations) {
		this.locations = locations;

		this.uiLocations = locations
				.stream()
				.map(t -> UILocationTransform.transform(t))
				.collect(Collectors.toList());
	}

	public void setMedications(List<Medication> medications) {
		this.medications = medications;

		this.uiMedications = medications
				.stream()
				.map(t -> UIMedicationTransform.transform(t))
				.collect(Collectors.toList());
	}

	public UIMedication getUIMedication(Reference reference) {
		String referenceId = ReferenceHelper.getReferenceId(reference, ResourceType.Medication);

		if (StringUtils.isEmpty(referenceId))
			return null;

		return this
				.uiMedications
				.stream()
				.filter(t -> t.getId().equals(referenceId))
				.collect(StreamExtension.firstOrNullCollector());
	}

	public void setMedicationStatements(UUID serviceId, UUID systemId, List<MedicationStatement> medicationStatements, ReferencedResources referencedResources) {
		this.medicationStatements = medicationStatements;

		this.uiMedicationStatements = medicationStatements
				.stream()
				.map(t -> UIMedicationStatementTransform.transform(serviceId, systemId, t, referencedResources))
				.collect(Collectors.toList());
	}

	public UIMedicationStatement getUIMedicationStatement(Reference reference) {
		String referenceId = ReferenceHelper.getReferenceId(reference, ResourceType.MedicationStatement);

		if (StringUtils.isEmpty(referenceId))
			return null;

		return this
				.uiMedicationStatements
				.stream()
				.filter(t -> t.getId().equals(referenceId))
				.collect(StreamExtension.firstOrNullCollector());
	}

	public void setObservations(UUID serviceId, UUID systemId, List<Observation> observations, ReferencedResources referencedResources) {
		this.observations = observations;
		this.uiObservations = observations
				.stream()
				.map(t -> UIObservationTransform.transform(serviceId, systemId, t, referencedResources))
				.collect(Collectors.toList());
	}

	public UIObservation getUIObservation(Reference reference) {
		String referenceId = ReferenceHelper.getReferenceId(reference, ResourceType.Observation);

		if (StringUtils.isEmpty(referenceId))
			return null;

		return this
				.uiObservations
				.stream()
				.filter(t -> t.getId().equals(referenceId))
				.collect(StreamExtension.firstOrNullCollector());
	}
}