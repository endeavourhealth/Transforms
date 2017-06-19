package org.endeavourhealth.transform.ui.models.resources.admin;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.endeavourhealth.transform.ui.models.resources.UIResource;
import org.endeavourhealth.transform.ui.models.types.*;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class UIPatient extends UIResource<UIPatient> {

	private UIInternalIdentifier patientId;
	private String nhsNumber;
	private UIHumanName name;
	private List<UIHumanName> otherNames;
	private UIDate dateOfBirth;
	private UIDate dateOfDeath;
	private String gender;
	private UIAddress homeAddress;
	private UICodeableConcept ethnicity;
	private List<UIContact> contacts;
	private List<UIContactPoint> telecoms;
	private List<String> localPatientIdentifiers;
	private UIOrganisation managingOrganisation;
	private UICodeableConcept religion;
	private UICodeableConcept maritalStatus;
	private UICodeableConcept language;
	private List<UIOrganisation> carerOrganisations;
	private List<UIPractitioner> carerPractitioners;

	public UIInternalIdentifier getPatientId() {
		return patientId;
	}

	public UIPatient setPatientId(UIInternalIdentifier patientId) {
		this.patientId = patientId;
		return this;
	}

	public UIHumanName getName() {
		return name;
	}

	public UIPatient setName(UIHumanName name) {
		this.name = name;
		return this;
	}

	public UIDate getDateOfBirth() {
		return dateOfBirth;
	}

	public UIPatient setDateOfBirth(UIDate dateOfBirth) {
		this.dateOfBirth = dateOfBirth;
		return this;
	}

	public String getGender() {
		return gender;
	}

	public UIPatient setGender(String gender) {
		this.gender = gender;
		return this;
	}

	public String getNhsNumber() {
		return nhsNumber;
	}

	public UIPatient setNhsNumber(String nhsNumber) {
		this.nhsNumber = nhsNumber;
		return this;
	}

	public UIAddress getHomeAddress() {
		return homeAddress;
	}

	public UIPatient setHomeAddress(UIAddress homeAddress) {
		this.homeAddress = homeAddress;
		return this;
	}

	public UICodeableConcept getEthnicity() {
		return ethnicity;
	}

	public UIPatient setEthnicity(UICodeableConcept ethnicity) {
		this.ethnicity = ethnicity;
		return this;
	}

	public List<UIContact> getContacts() {
		return contacts;
	}

	public UIPatient setContacts(List<UIContact> contacts) {
		this.contacts = contacts;
		return this;
	}

	public List<UIContactPoint> getTelecoms() {
		return telecoms;
	}

	public UIPatient setTelecoms(List<UIContactPoint> telecoms) {
		this.telecoms = telecoms;
		return this;
	}

	public UIDate getDateOfDeath() {
		return dateOfDeath;
	}

	public UIPatient setDateOfDeath(UIDate dateOfDeath) {
		this.dateOfDeath = dateOfDeath;
		return this;
	}

	public List<String> getLocalPatientIdentifiers() {
		return localPatientIdentifiers;
	}

	public UIPatient setLocalPatientIdentifiers(List<String> localPatientIdentifiers) {
		this.localPatientIdentifiers = localPatientIdentifiers;
		return this;
	}

	public UIOrganisation getManagingOrganisation() {
		return managingOrganisation;
	}

	public UIPatient setManagingOrganisation(UIOrganisation managingOrganisation) {
		this.managingOrganisation = managingOrganisation;
		return this;
	}

	public UICodeableConcept getReligion() {
		return religion;
	}

	public UIPatient setReligion(UICodeableConcept religion) {
		this.religion = religion;
		return this;
	}

	public UICodeableConcept getMaritalStatus() {
		return maritalStatus;
	}

	public UIPatient setMaritalStatus(UICodeableConcept maritalStatus) {
		this.maritalStatus = maritalStatus;
		return this;
	}

	public UICodeableConcept getLanguage() {
		return language;
	}

	public UIPatient setLanguage(UICodeableConcept language) {
		this.language = language;
		return this;
	}

	public List<UIOrganisation> getCarerOrganisations() {
		return carerOrganisations;
	}

	public UIPatient setCarerOrganisations(List<UIOrganisation> carerOrganisations) {
		this.carerOrganisations = carerOrganisations;
		return this;
	}

	public List<UIPractitioner> getCarerPractitioners() {
		return carerPractitioners;
	}

	public UIPatient setCarerPractitioners(List<UIPractitioner> carerPractitioners) {
		this.carerPractitioners = carerPractitioners;
		return this;
	}

	public List<UIHumanName> getOtherNames() {
		return otherNames;
	}

	public UIPatient setOtherNames(List<UIHumanName> otherNames) {
		this.otherNames = otherNames;
		return this;
	}
}
