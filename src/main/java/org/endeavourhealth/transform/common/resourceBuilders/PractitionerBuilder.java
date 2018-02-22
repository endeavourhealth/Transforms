package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.common.fhir.CodeableConceptHelper;
import org.endeavourhealth.common.fhir.FhirUri;
import org.endeavourhealth.common.fhir.FhirValueSetUri;
import org.endeavourhealth.common.fhir.PeriodHelper;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.*;

import java.util.Date;

public class PractitionerBuilder extends ResourceBuilderBase
                                 implements HasNameI {

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

    public void addRole() {
        this.practitioner.addPractitionerRole();
    }

    private Practitioner.PractitionerPractitionerRoleComponent getLastRole() {
        int size = this.practitioner.getPractitionerRole().size();
        return this.practitioner.getPractitionerRole().get(size-1);
    }

    private Coding getOrCreateCodeableConceptCodingOnLastRole() {
        Practitioner.PractitionerPractitionerRoleComponent role = getLastRole();
        CodeableConcept codeableConcept = role.getRole();
        if (codeableConcept == null) {
            codeableConcept = new CodeableConcept();
            role.setRole(codeableConcept);
        }

        Coding coding = CodeableConceptHelper.findCoding(codeableConcept, FhirValueSetUri.VALUE_SET_JOB_ROLE_CODES);
        if (coding == null) {
            coding = new Coding();
            coding.setSystem(FhirValueSetUri.VALUE_SET_JOB_ROLE_CODES);
            codeableConcept.addCoding(coding);
        }

        return coding;
    }

    private void calculateActiveState(CsvCell... sourceCells) {

        boolean isActive = false;

        //count the practitioner as active is ANY of their roles is active according to its Period
        for (Practitioner.PractitionerPractitionerRoleComponent role: this.practitioner.getPractitionerRole()) {
            Period period = role.getPeriod();
            if (period != null
                    && PeriodHelper.isActive(period)) {
                isActive = true;
            }
        }

        this.practitioner.setActive(isActive);

        auditValue("active", sourceCells);
    }

    public void setRoleStartDate(Date date, CsvCell... sourceCells) {
        Practitioner.PractitionerPractitionerRoleComponent role = getLastRole();

        Period period = role.getPeriod();
        if (period == null) {
            period = new Period();
            role.setPeriod(period);
        }
        period.setStart(date);

        //active state is only based on end date, so don't pass in our source cells
        calculateActiveState();

        int index = this.practitioner.getPractitionerRole().size()-1;
        auditValue("practitionerRole[" + index + "].period.start", sourceCells);
    }

    public void setRoleEndDate(Date date, CsvCell... sourceCells) {
        Practitioner.PractitionerPractitionerRoleComponent role = getLastRole();

        Period period = role.getPeriod();
        if (period == null) {
            period = new Period();
            role.setPeriod(period);
        }
        period.setEnd(date);

        calculateActiveState(sourceCells);

        int index = this.practitioner.getPractitionerRole().size()-1;
        auditValue("practitionerRole[" + index + "].period.end", sourceCells);
    }

    public void setRoleManagingOrganisation(Reference organisationReference, CsvCell... sourceCells) {
        Practitioner.PractitionerPractitionerRoleComponent role = getLastRole();
        role.setManagingOrganization(organisationReference);

        int index = this.practitioner.getPractitionerRole().size()-1;
        auditValue("practitionerRole[" + index + "].managingOrganization.reference", sourceCells);
    }

    public void setRoleName(String roleName, CsvCell... sourceCells) {
        Coding coding = getOrCreateCodeableConceptCodingOnLastRole();
        coding.setDisplay(roleName);

        int index = this.practitioner.getPractitionerRole().size()-1;
        auditValue("practitionerRole[" + index + "].role.coding[0].display", sourceCells);
    }

    public void setRoleCode(String roleCode, CsvCell... sourceCells) {
        Coding coding = getOrCreateCodeableConceptCodingOnLastRole();
        coding.setCode(roleCode);

        int index = this.practitioner.getPractitionerRole().size()-1;
        auditValue("practitionerRole[" + index + "].role.coding[0].code", sourceCells);
    }

    @Override
    public void addName(HumanName.NameUse use) {

        //practitioner only supports one name instance, so throw an error if we're trying to overwrite one
        if (this.practitioner.hasName()) {
            throw new IllegalArgumentException("Trying to set practitioner name when already set");
        }

        HumanName name = new HumanName();
        name.setUse(use);
        this.practitioner.setName(name);
    }

    @Override
    public HumanName getLastName() {
        return this.practitioner.getName();
    }

    @Override
    public String getLastNameJsonPrefix() {
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
