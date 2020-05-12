package org.endeavourhealth.transform.common.resourceBuilders;

import com.google.common.base.Strings;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.CodeableConcept;
import org.hl7.fhir.instance.model.Period;
import org.hl7.fhir.instance.model.Practitioner;
import org.hl7.fhir.instance.model.Reference;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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


    public static boolean removeRoleForId(PractitionerBuilder parentBuilder, String idValue) {
        if (Strings.isNullOrEmpty(idValue)) {
            throw new IllegalArgumentException("Can't remove role without ID");
        }

        List<Practitioner.PractitionerPractitionerRoleComponent> matches = new ArrayList<>();

        Practitioner practitioner = (Practitioner)parentBuilder.getResource();
        if (!practitioner.hasPractitionerRole()) {
            return false;
        }
        List<Practitioner.PractitionerPractitionerRoleComponent> roles = practitioner.getPractitionerRole();
        for (Practitioner.PractitionerPractitionerRoleComponent role: roles) {
            //if we match on ID, then remove this name from the parent object
            if (role.hasId()
                    && role.getId().equals(idValue)) {
                matches.add(role);
            }
        }

        if (matches.isEmpty()) {
            return false;

        } else if (matches.size() > 1) {
            throw new IllegalArgumentException("Found " + matches.size() + " roles for ID " + idValue);

        } else {
            Practitioner.PractitionerPractitionerRoleComponent role = matches.get(0);

            //remove any audits we've created for the Name
            String identifierJsonPrefix = parentBuilder.getRoleJsonPrefix(role);
            parentBuilder.getAuditWrapper().removeAudit(identifierJsonPrefix);

            practitioner.getPractitionerRole().remove(role);
            return true;
        }
    }

    public void setId(String id, CsvCell... sourceCells) {
        this.role.setId(id);

        auditValue("id", sourceCells);
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
                audit.auditValue(csvCell.getPublishedFileId(), csvCell.getRecordNumber(), csvCell.getColIndex(), jsonField);
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
    public CodeableConcept createNewCodeableConcept(CodeableConceptBuilder.Tag tag, boolean useExisting) {
        if (tag == CodeableConceptBuilder.Tag.Practitioner_Role) {
            if (role.hasRole()) {
                if (useExisting) {
                    return role.getRole();
                } else {
                    throw new IllegalArgumentException("Trying to set role on Practitioner Role when it already has one");
                }
            }
            CodeableConcept codeableConcept = new CodeableConcept();
            role.setRole(codeableConcept);
            return codeableConcept;

        } else if (tag == CodeableConceptBuilder.Tag.Practitioner_Specialty) {
            //although the object supports multiple specialties, we only support having one
            if (role.hasSpecialty()) {
                if (useExisting) {
                    return role.getSpecialty().get(0);
                } else {
                    throw new IllegalArgumentException("Trying to set specialty on Practitioner Role when it already has one");
                }
            }

            return role.addSpecialty();

        } else {
            throw new IllegalArgumentException("Unknown tag [" + tag + "]");
        }
    }

    public CodeableConcept getSpecialty() {
        if (role.hasSpecialty()) {
            return role.getSpecialty().get(0);
        } else {
            return null;
        }
    }

    public CodeableConcept getRole() {
        if (role.hasRole()) {
            return role.getRole();
        } else {
            return null;
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
