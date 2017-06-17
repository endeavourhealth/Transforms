package org.endeavourhealth.transform.ui.models.resources.admin;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.endeavourhealth.transform.ui.models.resources.UIResource;
import org.endeavourhealth.transform.ui.models.types.UIAddress;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class UIOrganisation extends UIResource<UIOrganisation> {
	private String name;
	private String type;
	private String odsCode;
	private UIAddress address;

	public String getName() {
		return name;
	}

	public UIOrganisation setName(String name) {
		this.name = name;
		return this;
	}

	public String getType() {
		return type;
	}

	public UIOrganisation setType(String type) {
		this.type = type;
		return this;
	}

	public UIAddress getAddress() {
		return address;
	}

	public UIOrganisation setAddress(UIAddress address) {
		this.address = address;
		return this;
	}

	public String getOdsCode() {
		return odsCode;
	}

	public UIOrganisation setOdsCode(String odsCode) {
		this.odsCode = odsCode;
		return this;
	}
}
