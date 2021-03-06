package org.endeavourhealth.transform.common.resourceBuilders;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.CodingHelper;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.CodeableConcept;
import org.hl7.fhir.instance.model.Coding;

public class CodeableConceptBuilder {

    private HasCodeableConceptI parentBuilder = null;
    private CodeableConcept codeableConcept = null;
    private Tag tag = null;

    public enum Tag {
        Practitioner_Role,
        Practitioner_Specialty,
        Condition_Main_Code,
        Procedure_Main_Code,
        Observation_Main_Code,
        Observation_Component_Code,
        Observation_Range_Meaning,
        Referral_Request_Service,
        Referral_Request_Priority,
        Patient_Language,
        //Patient_Religion,
        Encounter_Specialty,
        Encounter_Treatment_Function,
        Encounter_Source,
        //Encounter_Admission_Type,
        Encounter_Location_Type,
        Immunization_Main_Code,
        Immunization_Site,
        Immunization_Route,
        //Patient_Contact_Relationship,
        Allergy_Intolerance_Main_Code,
        Specimen_Main_Code,
        Appointment_Dna_Reason_Code,
        Family_Member_History_Main_Code,
        Diagnostic_Order_Main_Code,
        Diagnostic_Report_Main_Code,
        Procedure_Request_Main_Code,
        Medication_Order_Drug_Code,
        Medication_Statement_Drug_Code,
        Location_Type,
        Composition_Type,
        Encounter_Patient_Class_Other,
        Encounter_AE_Attendance_Category,
        Encounter_AE_Attendance_Source,
        Encounter_AE_Arrival_Mode,
        Encounter_Admission_Method,
        Encounter_Admission_Source,
        Encounter_Admission_Ward,
        Encounter_Discharge_Method,
        Encounter_Discharge_Destination,
        Encounter_Discharge_Ward,
        Encounter_Admin_Category,
        Encounter_Appointment_Attended,
        Encounter_Appointment_Outcome,
    }


    public CodeableConceptBuilder(HasCodeableConceptI parentBuilder, Tag tag) {
        this(parentBuilder, tag, false);
    }

    public CodeableConceptBuilder(HasCodeableConceptI parentBuilder, Tag tag, boolean useExisting) {

        if (parentBuilder == null) {
            throw new IllegalArgumentException("Null parentBuilder in CodeableConceptBuilder constructor");
        }
        if (tag == null) {
            throw new IllegalArgumentException("Null tag in CodeableConceptBuilder constructor");
        }

        this.parentBuilder = parentBuilder;
        this.tag = tag;
        this.codeableConcept = parentBuilder.createNewCodeableConcept(tag, useExisting);
    }

    public static void removeExistingCodeableConcept(HasCodeableConceptI parentBuilder, Tag tag, CodeableConcept codeableConcept) {

        //remove any audits we've created for the CodeableConcept
        String identifierJsonPrefix = parentBuilder.getCodeableConceptJsonPath(tag, codeableConcept);
        parentBuilder.getAuditWrapper().removeAudit(identifierJsonPrefix);

        //and remove the CodeableConcept itself
        parentBuilder.removeCodeableConcept(tag, codeableConcept);
    }

    public void addCoding(String systemUrl, CsvCell... sourceCells) {
        Coding coding = CodingHelper.createCoding(systemUrl, null, null);
        this.codeableConcept.addCoding(coding);

        addCodingAudit(coding, "system", sourceCells);
    }

    public void setId(String id, CsvCell... sourceCells) {
        this.codeableConcept.setId(id);

        addAudit("id", sourceCells);
    }

    public void setCodingCode(String code, CsvCell... sourceCells) {
        if (Strings.isNullOrEmpty(code)) {
            return;
        }
        Coding coding = getLastCoding(codeableConcept);
        coding.setCode(code);

        addCodingAudit(coding, "code", sourceCells);
    }

    public void setCodingDisplay(String display, CsvCell... sourceCells) {
        if (Strings.isNullOrEmpty(display)) {
            return;
        }

        Coding coding = getLastCoding(codeableConcept);
        coding.setDisplay(display);

        addCodingAudit(coding, "display", sourceCells);
    }

    private Coding getLastCoding(CodeableConcept codeableConcept) {
        if (!codeableConcept.hasCoding()) {
            throw new IllegalArgumentException("Must call addCoding before setting code or disply");
        }
        int index = codeableConcept.getCoding().size()-1;
        return codeableConcept.getCoding().get(index);
    }

    public boolean hasCoding() {
        return codeableConcept.hasCoding();
    }

    private void addCodingAudit(Coding coding, String codingElement, CsvCell... sourceCells) {
        int index = codeableConcept.getCoding().indexOf(coding);
        String jsonSuffix = "coding[" + index + "]." + codingElement;

        addAudit(jsonSuffix, sourceCells);
    }

    private void addAudit(String jsonSuffix, CsvCell... sourceCells) {
        String jsonField = this.parentBuilder.getCodeableConceptJsonPath(tag, codeableConcept) + "." + jsonSuffix;
        ResourceFieldMappingAudit audit = this.parentBuilder.getAuditWrapper();
        for (CsvCell csvCell: sourceCells) {
            if (csvCell != null) {
                audit.auditValue(csvCell.getPublishedFileId(), csvCell.getRecordNumber(), csvCell.getColIndex(), jsonField);
            }
        }
    }

    public void setText(String textValue, CsvCell... sourceCells) {
        if (Strings.isNullOrEmpty(textValue)) {
            return;
        }

        codeableConcept.setText(textValue);

        addAudit("text", sourceCells);
    }


    public CodeableConcept getCodeableConcept() {
        return codeableConcept;
    }

    /**
     * replaces any instance of the "text" field with the new term
     */
    public void replaceText(String newTerm, CsvCell... sourceCells) {
        if (Strings.isNullOrEmpty(newTerm)) {
            return;
        }

        String toReplace = codeableConcept.getText();

        //update the codings
        if (codeableConcept.hasCoding()) {
            for (Coding coding: codeableConcept.getCoding()) {
                String codingDisplay = coding.getDisplay();

                //only update codings that have the same text as the "text" element, so we don't accidentally
                //update official Snomed terms or anything similar
                if ((codingDisplay == null && toReplace == null)
                        || (codingDisplay != null && toReplace != null && codingDisplay.equals(toReplace))) {

                    coding.setDisplay(newTerm);
                    addCodingAudit(coding, "display", sourceCells);
                }
            }
        }

        //finally update the text element
        setText(newTerm, sourceCells);
    }

    /*public void addCodeableConceptNoAudit(CodeableConcept otherConcept) {

        if (otherConcept.hasId()) {
            throw new RuntimeException("Id field not supported when adding codeable concepts");
        }

        if (otherConcept.hasCoding()) {
            for (Coding otherCoding: otherConcept.getCoding()) {
                addCoding(otherCoding.getSystem());
                if (otherCoding.hasCode()) {
                    setCodingCode(otherCoding.getCode());
                }
                if (otherCoding.hasDisplay()) {
                    setCodingDisplay(otherCoding.getDisplay());
                }
            }
        }

        if (otherConcept.hasText()) {
            setText(otherConcept.getText());
        }
    }*/
}
