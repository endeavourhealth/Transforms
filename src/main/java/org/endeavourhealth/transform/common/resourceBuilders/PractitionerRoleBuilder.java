package org.endeavourhealth.transform.common.resourceBuilders;

import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.CodeableConcept;
import org.hl7.fhir.instance.model.Period;
import org.hl7.fhir.instance.model.Practitioner;
import org.hl7.fhir.instance.model.Reference;

import java.util.Date;

public class PractitionerRoleBuilder implements HasCodeableConceptI {

    private PractitionerBuilder parentBuilder = null;
    private Practitioner.PractitionerPractitionerRoleComponent role = null;

    public PractitionerRoleBuilder(PractitionerBuilder parentBuilder) {
        this(parentBuilder, null);
    }

    public PractitionerRoleBuilder(PractitionerBuilder parentBuilder, Practitioner.PractitionerPractitionerRoleComponent role) {
        this.parentBuilder = parentBuilder;
        this.role = role;

        if (this.role == null) {
            this.role = parentBuilder.addRole();
        }
    }

    public void setRoleStartDate(Date date, CsvCell... sourceCells) {
        Period period = role.getPeriod();
        if (period == null) {
            period = new Period();
            role.setPeriod(period);
        }
        period.setStart(date);

        auditValue("period.start", sourceCells);
    }

    private void auditValue(String jsonSuffix, CsvCell... sourceCells) {
        String jsonField = parentBuilder.getRoleJsonPrefix(role) + "." + jsonSuffix;

        ResourceFieldMappingAudit audit = this.parentBuilder.getAuditWrapper();
        for (CsvCell csvCell: sourceCells) {
            if (csvCell != null) {
                audit.auditValue(csvCell.getRowAuditId(), csvCell.getColIndex(), jsonField);
            }
        }
    }

    public void setRoleEndDate(Date date, CsvCell... sourceCells) {
        Period period = role.getPeriod();
        if (period == null) {
            period = new Period();
            role.setPeriod(period);
        }
        period.setEnd(date);

        auditValue("period.end", sourceCells);
    }

    public void setRoleManagingOrganisation(Reference organisationReference, CsvCell... sourceCells) {
        role.setManagingOrganization(organisationReference);

        auditValue("managingOrganization.reference", sourceCells);
    }

    /*public void setRoleName(String roleName, CsvCell... sourceCells) {
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

    private Coding getOrCreateCodeableConceptCodingOnLastRole() {
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
    }*/


    @Override
    public CodeableConcept createNewCodeableConcept(CodeableConceptBuilder.Tag tag) {
        if (tag == CodeableConceptBuilder.Tag.Practitioner_Role) {
            if (role.hasRole()) {
                throw new IllegalArgumentException("Trying to set role on Practitioner Role when it already has one");
            }
            CodeableConcept codeableConcept = new CodeableConcept();
            role.setRole(codeableConcept);
            return codeableConcept;

        } else if (tag == CodeableConceptBuilder.Tag.Practitioner_Specialty) {
            return role.addSpecialty();

        } else {
            throw new IllegalArgumentException("Unknown tag [" + tag + "]");
        }
    }

    @Override
    public String getCodeableConceptJsonPath(CodeableConceptBuilder.Tag tag, CodeableConcept codeableConcept) {
        if (tag == CodeableConceptBuilder.Tag.Practitioner_Role) {
            return parentBuilder.getRoleJsonPrefix(role) + ".role";

        } else if (tag == CodeableConceptBuilder.Tag.Practitioner_Specialty) {
            int index = role.getSpecialty().indexOf(codeableConcept);
            return parentBuilder.getRoleJsonPrefix(role) + ".specialty[" + index + "]";

        } else {
            throw new IllegalArgumentException("Unknown tag [" + tag + "]");
        }
    }

    @Override
    public ResourceFieldMappingAudit getAuditWrapper() {
        return parentBuilder.getAuditWrapper();
    }

    @Override
    public void removeCodeableConcept(CodeableConceptBuilder.Tag tag, CodeableConcept codeableConcept) {
        if (tag == CodeableConceptBuilder.Tag.Practitioner_Role) {
            role.setRole(null);

        } else if (tag == CodeableConceptBuilder.Tag.Practitioner_Specialty) {
            role.getSpecialty().clear();

        } else {
            throw new IllegalArgumentException("Unknown tag [" + tag + "]");
        }
    }
}
