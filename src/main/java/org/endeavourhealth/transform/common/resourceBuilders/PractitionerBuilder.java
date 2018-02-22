package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.common.fhir.PeriodHelper;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

public class PractitionerBuilder extends ResourceBuilderBase
                                 implements HasNameI, HasIdentifierI, HasContactPointI, HasAddressI {

    private Practitioner practitioner = null;

    public PractitionerBuilder() {
        this(null);
    }

    public PractitionerBuilder(Practitioner practitioner) {
        this.practitioner = practitioner;
        if (this.practitioner == null) {
            this.practitioner = new Practitioner();
            this.practitioner.setMeta(new Meta().addProfile(FhirUri.PROFILE_URI_PRACTITIONER));
        }
    }


    @Override
    public DomainResource getResource() {
        return practitioner;
    }

    public Practitioner.PractitionerPractitionerRoleComponent addRole() {
        return this.practitioner.addPractitionerRole();
    }

    public String getRoleJsonPrefix(Practitioner.PractitionerPractitionerRoleComponent role) {
        int index = this.practitioner.getPractitionerRole().indexOf(role);
        return "practitionerRole[" + index + "]";
    }


    public void setActive(boolean isActive, CsvCell... sourceCells) {
        this.practitioner.setActive(isActive);

        auditValue("active", sourceCells);
    }


    public void calculateActiveFromRoles() {

        boolean active = false;
        for (Practitioner.PractitionerPractitionerRoleComponent role: this.practitioner.getPractitionerRole()) {
            Period period = role.getPeriod();
            if (period != null
                    && PeriodHelper.isActive(period)) {
                active = true;
            }
        }

        setActive(active);
    }

    @Override
    public Identifier addIdentifier() {
        return this.practitioner.addIdentifier();
    }

    @Override
    public String getIdentifierJsonPrefix(Identifier identifier) {
        int index = this.practitioner.getIdentifier().indexOf(identifier);
        return "identifier[" + index + "]";
    }

    @Override
    public ContactPoint addContactPoint() {
        return this.practitioner.addTelecom();
    }

    @Override
    public String getContactPointJsonPrefix(ContactPoint contactPoint) {
        int index = this.practitioner.getTelecom().indexOf(contactPoint);
        return "telecom[" + index + "]";
    }

    @Override
    public Address addAddress() {
        return this.practitioner.addAddress();
    }

    @Override
    public String getAddressJsonPrefix(Address address) {
        int index = this.practitioner.getAddress().indexOf(address);
        return "address[" + index + "]";
    }

    @Override
    public HumanName addName() {
        if (this.practitioner.hasName()) {
            throw new IllegalArgumentException("Trying to set name on practitioner that already has one");
        }
        HumanName name = new HumanName();
        this.practitioner.setName(name);
        return name;
    }

    @Override
    public String getNameJsonPrefix(HumanName name) {
        return "name";
    }



    /*@Override
    public void addNamePrefix(String prefix, CsvCell... sourceCells) {
        HumanName name = this.practitioner.getName();
        name.addPrefix(prefix);

        int index = name.getPrefix().size()-1;
        auditValue("name.prefix[" + index + "]", sourceCells);
    }

    @Override
    public void addNameGiven(String given, CsvCell... sourceCells) {
        HumanName name = this.practitioner.getName();
        name.addGiven(given);

        int index = name.getGiven().size()-1;
        auditValue("name.given[" + index + "]", sourceCells);
    }

    @Override
    public void addNameFamily(String family, CsvCell... sourceCells) {
        HumanName name = this.practitioner.getName();
        name.addFamily(family);

        int index = name.getFamily().size()-1;
        auditValue("name.family[" + index + "]", sourceCells);
    }

    @Override
    public void addNameSuffix(String suffix, CsvCell... sourceCells) {
        HumanName name = this.practitioner.getName();
        name.addSuffix(suffix);

        int index = name.getFamily().size()-1;
        auditValue("name.suffix[" + index + "]", sourceCells);
    }

    @Override
    public void addNameDisplayName(String displayName, CsvCell... sourceCells) {
        HumanName name = this.practitioner.getName();
        name.setText(displayName);

        auditValue("name.text", sourceCells);
    }*/


}
