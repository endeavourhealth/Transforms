package org.endeavourhealth.transform.ui.transforms.admin;

import org.endeavourhealth.common.fhir.FhirExtensionUri;
import org.endeavourhealth.transform.ui.helpers.*;
import org.endeavourhealth.transform.ui.models.resources.admin.UIContact;
import org.endeavourhealth.transform.ui.models.types.UIAddress;
import org.endeavourhealth.transform.ui.models.types.UICodeableConcept;
import org.endeavourhealth.transform.ui.models.types.UIDate;
import org.endeavourhealth.transform.ui.models.types.UIHumanName;
import org.hl7.fhir.instance.model.CodeableConcept;
import org.hl7.fhir.instance.model.DateTimeType;
import org.hl7.fhir.instance.model.Patient;

import java.util.ArrayList;
import java.util.List;

public class UIContactTransform {

	public static List<UIContact> transform(List<Patient.ContactComponent> contactList) {

		List<UIContact> contacts = new ArrayList<>();

		for (Patient.ContactComponent contact : contactList) {
			contacts.add(transform(contact));
		}

		return contacts;
	}

	private static UIContact transform(Patient.ContactComponent contact) {

		UIHumanName name = NameHelper.transform(contact.getName());
		UIAddress homeAddress = AddressHelper.transform(contact.getAddress());

		return new UIContact()
				.setName(name)
				.setDateOfBirth(getBirthDate(contact))
				.setHomeAddress(homeAddress)
				.setRelationships(getRelationships(contact));
	}

	private static UIDate getBirthDate(Patient.ContactComponent contact) {
		DateTimeType contactDob = ExtensionHelper.getExtensionValue(contact, FhirExtensionUri.PATIENT_CONTACT_DOB, DateTimeType.class);
		if (contactDob == null)
			return null;

		return DateHelper.convert(contactDob);
	}

	private static List<UICodeableConcept> getRelationships(Patient.ContactComponent contact) {
		List<UICodeableConcept> relationships = new ArrayList<>();

		for(CodeableConcept relationship : contact.getRelationship()) {
			relationships.add(CodeHelper.convert(relationship));
		}

		return relationships;
	}

}
