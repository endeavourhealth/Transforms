package org.endeavourhealth.transform.ui.models.resources.admin;

import org.endeavourhealth.transform.ui.models.types.UIAddress;
import org.endeavourhealth.transform.ui.models.types.UICodeableConcept;
import org.endeavourhealth.transform.ui.models.types.UIDate;
import org.endeavourhealth.transform.ui.models.types.UIHumanName;

import java.util.List;

public class UIContact {
	private UIHumanName name;
	private UIDate dateOfBirth;
	private List<UICodeableConcept> relationships;
	private UIAddress homeAddress;

	public UIDate getDateOfBirth() {
		return dateOfBirth;
	}

	public UIContact setDateOfBirth(UIDate dateOfBirth) {
		this.dateOfBirth = dateOfBirth;
		return this;
	}

	public List<UICodeableConcept> getRelationships() {
		return relationships;
	}

	public UIContact setRelationships(List<UICodeableConcept> relationships) {
		this.relationships = relationships;
		return this;
	}

	public UIHumanName getName() {
		return name;
	}

	public UIContact setName(UIHumanName name) {
		this.name = name;
		return this;
	}

	public UIAddress getHomeAddress() {
		return homeAddress;
	}

	public UIContact setHomeAddress(UIAddress homeAddress) {
		this.homeAddress = homeAddress;
		return this;
	}
}
