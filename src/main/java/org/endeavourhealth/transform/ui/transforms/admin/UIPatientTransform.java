package org.endeavourhealth.transform.ui.transforms.admin;

import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.common.fhir.ReferenceHelper;
import org.endeavourhealth.transform.ui.helpers.*;
import org.endeavourhealth.transform.ui.helpers.ExtensionHelper;
import org.endeavourhealth.transform.ui.models.resources.admin.*;
import org.endeavourhealth.transform.ui.models.types.*;
import org.hl7.fhir.instance.model.*;

import java.util.*;

public class UIPatientTransform {

	private static Set<String> identifierUris = new HashSet<>(Arrays.asList(
			FhirUri.IDENTIFIER_SYSTEM_HOMERTON_CNN_PATIENT_ID,
			FhirUri.IDENTIFIER_SYSTEM_BARTS_MRN_PATIENT_ID));

	public static UIPatient transform(UUID serviceId, Patient patient, ReferencedResources referencedResources) {

		UIHumanName name = NameHelper.getUsualOrOfficialName(patient.getName());
		UIAddress homeAddress = AddressHelper.getHomeAddress(patient.getAddress());

		return new UIPatient()
				.setId(patient.getId())
				.setNhsNumber(getNhsNumber(patient.getIdentifier()))
				.setName(name)
				.setOtherNames(getAllNames(patient.getName()))
				.setDateOfBirth(getBirthDate(patient))
				.setDateOfDeath(getDeathDate(patient))
				.setGender(patient.getGender().toCode())
				.setHomeAddress(homeAddress)
				.setEthnicity(getEthnicity(patient))
				.setContacts(getContacts(patient.getContact()))
				.setTelecoms(getTelecoms(patient.getTelecom()))
				.setLocalPatientIdentifiers(getLocalPatientIdentifers(patient.getIdentifier()))
				.setManagingOrganisation(getManagingOrganisation(patient.getManagingOrganization(), referencedResources))
				.setMaritalStatus(CodeHelper.convert(patient.getMaritalStatus()))
				.setLanguage(getLanguage(patient.getCommunication()))
				.setReligion(getReligion(patient))
				.setCarerOrganisations(getCarerOrganisations(patient.getCareProvider(), referencedResources))
				.setCarerPractitioners(getCarerPractitioners(serviceId, patient.getCareProvider()));
	}

	private static List<UIHumanName> getAllNames(List<HumanName> names) {
		List<UIHumanName> result = new ArrayList<>();
		for (HumanName name : names) {
			result.add(NameHelper.transform(name));
		}

		return result;
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

	private static List<UIInternalIdentifier> getCarerPractitioners(UUID serviceId, List<Reference> careProviders) {
		List<UIInternalIdentifier> uiPractitioners = new ArrayList<>();

		for(Reference reference : careProviders) {
			if (ReferenceHelper.isResourceType(reference, ResourceType.Practitioner)) {

				String referenceId = ReferenceHelper.getReferenceId(reference, ResourceType.Practitioner);

				if (referenceId != null) {
					UIInternalIdentifier practitioner = new UIInternalIdentifier()
							.setServiceId(serviceId)
							.setResourceId(UUID.fromString(referenceId));

					uiPractitioners.add(practitioner);
				}
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

	private static List<String> getLocalPatientIdentifers(List<Identifier> identifiers) {


		List<String> localIds = new ArrayList<>();

		for (Identifier identifier : identifiers) {
			if (identifier.getSystem() != null && identifierUris.contains(identifier.getSystem())) {
				localIds.add(identifier.getValue());
			}
		}

		return localIds;
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
