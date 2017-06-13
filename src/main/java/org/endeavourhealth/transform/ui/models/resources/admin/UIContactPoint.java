package org.endeavourhealth.transform.ui.models.resources.admin;

public class UIContactPoint {
	private String system;
	private String value;
	private String use;

	public String getSystem() {
		return system;
	}

	public UIContactPoint setSystem(String system) {
		this.system = system;
		return this;
	}

	public String getValue() {
		return value;
	}

	public UIContactPoint setValue(String value) {
		this.value = value;
		return this;
	}

	public String getUse() {
		return use;
	}

	public UIContactPoint setUse(String use) {
		this.use = use;
		return this;
	}
}
