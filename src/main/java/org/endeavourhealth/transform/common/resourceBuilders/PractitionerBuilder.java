package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.common.fhir.FhirProfileUri;
import org.endeavourhealth.common.fhir.PeriodHelper;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

import java.util.ArrayList;
import java.util.List;

public class PractitionerBuilder extends ResourceBuilderBase
                                 implements HasNameI, HasIdentifierI, HasContactPointI, HasAddressI {

    private Practitioner practitioner = null;

    public PractitionerBuilder() {
        this(null);
    }

    public PractitionerBuilder(Practitioner practitioner) {
        this(practitioner, null);
    }

    public PractitionerBuilder(Practitioner practitioner, ResourceFieldMappingAudit audit) {
        super(audit);

        this.practitioner = practitioner;
        if (this.practitioner == null) {
            this.practitioner = new Practitioner();
            this.practitioner.setMeta(new Meta().addProfile(FhirProfileUri.PROFILE_URI_PRACTITIONER));
        }
    }


    @Override
    public DomainResource getResource() {
        return practitioner;
    }

    public Practitioner.PractitionerPractitionerRoleComponent addRole() {
        return this.practitioner.addPractitionerRole();
    }

    public Practitioner.PractitionerPractitionerRoleComponent getRole() {
        if (this.practitioner.hasPractitionerRole()) {
            return this.practitioner.getPractitionerRole().get(0);
        } else {
            return null;
        }
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
    public List<ContactPoint> getContactPoint() {
        return this.practitioner.getTelecom();
    }

    @Override
    public void removeContactPoint(ContactPoint contactPoint) {
        this.practitioner.getTelecom().remove(contactPoint);
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
    public List<Address> getAddresses() {
        return this.practitioner.getAddress();
    }

    @Override
    public void removeAddress(Address address) {
        this.practitioner.getAddress().remove(address);
    }

    @Override
    public HumanName addName() {
        // remove existing name
        if (this.practitioner.hasName()) {
            this.practitioner.setName(null);
        }
        HumanName name = new HumanName();
        this.practitioner.setName(name);
        return name;
    }

    @Override
    public String getNameJsonPrefix(HumanName name) {
        return "name";
    }

    @Override
    public List<HumanName> getNames() {
        List<HumanName> ret = new ArrayList<>();
        if (this.practitioner.hasName()) {
            ret.add(this.practitioner.getName());
        }
        return ret;
    }

    @Override
    public void removeName(HumanName name) {
        this.practitioner.setName(null);
    }

    @Override
    public List<Identifier> getIdentifiers() {
        return this.practitioner.getIdentifier();
    }

    @Override
    public void removeIdentifier(Identifier identifier) {
        this.practitioner.getIdentifier().remove(identifier);
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
