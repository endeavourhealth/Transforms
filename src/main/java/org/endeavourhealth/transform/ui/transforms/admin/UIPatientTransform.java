package org.endeavourhealth.transform.ui.transforms.admin;

import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.ui.helpers.*;
import org.endeavourhealth.transform.ui.helpers.ExtensionHelper;
import org.endeavourhealth.transform.ui.models.resources.admin.*;
import org.endeavourhealth.transform.ui.models.types.*;
import org.hl7.fhir.instance.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class UIPatientTransform {

	public static UIPatient transform(Patient patient, ReferencedResources referencedResources) {

		UIHumanName name = NameHelper.getUsualOrOfficialName(patient.getName());
		UIAddress homeAddress = AddressHelper.getHomeAddress(patient.getAddress());

		return new UIPatient()
				.setId(patient.getId())
				.setNhsNumber(getNhsNumber(patient.getIdentifier()))
				.setName(name)
				.setDateOfBirth(getBirthDate(patient))
				.setDateOfDeath(getDeathDate(patient))
				.setGender(patient.getGender().toCode())
				.setHomeAddress(homeAddress)
				.setEthnicity(getEthnicity(patient))
				.setContacts(getContacts(patient.getContact()))
				.setTelecoms(getTelecoms(patient.getTelecom()))
				.setLocalPatientIdentifier(getLocalPatientIdentifer(patient.getIdentifier()))
				.setManagingOrganisation(getManagingOrganisation(patient.getManagingOrganization(), referencedResources))
				.setMaritalStatus(CodeHelper.convert(patient.getMaritalStatus()))
				.setLanguage(getLanguage(patient.getCommunication()))
				.setReligion(getReligion(patient))
				.setCarerOrganisations(getCarerOrganisations(patient.getCareProvider(), referencedResources))
				.setCarerPractitioners(getCarerPractitioners(patient.getCareProvider(), referencedResources));
	}

	private static List<UIOrganisation> getCarerOrganisations(List<Reference> careProviders, ReferencedResources referencedResources) {
		List<UIOrganisation> uiOrganisations = new ArrayList<>();

		for(Reference reference : careProviders) {
			if (ReferenceHelper.isResourceType(reference, ResourceType.Organization)) {
				UIOrganisation uiOrganisation = referencedResources.getUIOrganisation(reference);
				if (uiOrganisation != null)
					uiOrganisations.add(uiOrganisation);
			}
		}

		return uiOrganisations;
	}

	private static List<UIPractitioner> getCarerPractitioners(List<Reference> careProviders, ReferencedResources referencedResources) {
		List<UIPractitioner> uiPractitioners = new ArrayList<>();

		for(Reference reference : careProviders) {
			if (ReferenceHelper.isResourceType(reference, ResourceType.Practitioner)) {
				UIPractitioner uiPractitioner = referencedResources.getUIPractitioner(reference);
				if (uiPractitioner != null)
					uiPractitioners.add(uiPractitioner);
			}
		}

		return uiPractitioners;
	}

	private static UICodeableConcept getReligion(Patient patient) {
		CodeableConcept religion = ExtensionHelper.getExtensionValue(patient, FhirExtensionUri.PATIENT_RELIGION, CodeableConcept.class);

		if (religion != null)
			return CodeHelper.convert(religion);

		return null;
	}

	private static UICodeableConcept getLanguage(List<Patient.PatientCommunicationComponent> communication) {
		Optional<Patient.PatientCommunicationComponent> lang = communication.stream().filter(c -> c.hasLanguage()).findFirst();

		if (lang.isPresent())
			return CodeHelper.convert(lang.get().getLanguage());

		return null;
	}

	private static UIOrganisation getManagingOrganisation(Reference managingOrganization, ReferencedResources referencedResources) {
		return referencedResources.getUIOrganisation(managingOrganization);
	}

	private static String getLocalPatientIdentifer(List<Identifier> identifiers) {
		String identifierUris[] = {FhirUri.IDENTIFIER_SYSTEM_HOMERTON_CNN_PATIENT_ID};

		for (String uri : identifierUris) {
			String result = IdentifierHelper.getIdentifierBySystem(identifiers, uri);
			if (result != null)
				return result;
		}

		return null;
	}

	private static UIDate getBirthDate(Patient patient) {
		if (!patient.hasBirthDate())
			return null;

		return DateHelper.convert(patient.getBirthDateElement());
	}

	private static UIDate getDeathDate(Patient patient) {
		try {
			if (!patient.hasDeceasedDateTimeType())
				return null;

			return DateHelper.convert(patient.getDeceasedDateTimeType());
		} catch (Exception e) {
			return null;
		}
	}


	private static String getNhsNumber(List<Identifier> identifiers) {
		String result = IdentifierHelper.getIdentifierBySystem(identifiers, FhirUri.IDENTIFIER_SYSTEM_NHSNUMBER);

		if (result != null)
			result = result.replaceAll(" ", "");

		return result;
	}

	private static UICodeableConcept getEthnicity(Patient patient) {
		CodeableConcept ethnicity = ExtensionHelper.getExtensionValue(patient, FhirExtensionUri.PATIENT_ETHNICITY, CodeableConcept.class);
		return CodeHelper.convert(ethnicity);
	}

	private static List<UIContact> getContacts(List<Patient.ContactComponent> contacts) {
		return UIContactTransform.transform(contacts);
	}

	private static List<UIContactPoint> getTelecoms(List<ContactPoint> contactPoints) {
		return UIContactPointTransform.transform(contactPoints);
	}
}
