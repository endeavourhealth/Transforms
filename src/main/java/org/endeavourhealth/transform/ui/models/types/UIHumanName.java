package org.endeavourhealth.transform.ui.models.types;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class UIHumanName {
    private String familyName;
    private String[] givenNames;
    private String prefix;
    private String text;
    private String use;

    public String getFamilyName() {
        return familyName;
    }

    public UIHumanName setFamilyName(String familyName) {
        this.familyName = familyName;
        return this;
    }

    public String[] getGivenNames() {
        return givenNames;
    }

    public UIHumanName setGivenNames(List<String> givenNames) {
        this.givenNames = givenNames.toArray(new String[givenNames.size()]);
        return this;
    }

    public String getPrefix() {
        return prefix;
    }

    public UIHumanName setPrefix(String prefix) {
        this.prefix = prefix;
        return this;
    }

	public String getText() {
		return text;
	}

	public UIHumanName setText(String text) {
		this.text = text;
		return this;
	}

	public String getUse() {
		return use;
	}

	public UIHumanName setUse(String use) {
		this.use = use;
		return this;
	}
}
