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
    private String codeableConceptTag = null;

    public CodeableConceptBuilder(HasCodeableConceptI parentBuilder, String codeableConceptTag) {
        this(parentBuilder, codeableConceptTag, null);
    }

    public CodeableConceptBuilder(HasCodeableConceptI parentBuilder, String codeableConceptTag, CodeableConcept codeableConcept) {
        this.parentBuilder = parentBuilder;
        this.codeableConcept = codeableConcept;
        this.codeableConceptTag = codeableConceptTag;

        if (this.codeableConcept == null) {
            this.codeableConcept = parentBuilder.createNewCodeableConcept(codeableConceptTag);
        }
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

    private void addCodingAudit(Coding coding, String codingElement, CsvCell... sourceCells) {
        int index = codeableConcept.getCoding().indexOf(coding);
        String jsonSuffix = "coding[" + index + "]." + codingElement;

        addAudit(jsonSuffix, sourceCells);
    }

    private void addAudit(String jsonSuffix, CsvCell... sourceCells) {
        String jsonField = this.parentBuilder.getCodeableConceptJsonPath(codeableConceptTag, codeableConcept) + "." + jsonSuffix;
        ResourceFieldMappingAudit audit = this.parentBuilder.getAuditWrapper();
        for (CsvCell csvCell: sourceCells) {
            audit.auditValue(csvCell.getRowAuditId(), csvCell.getColIndex(), jsonField);
        }
    }

    public void setText(String textValue, CsvCell... sourceCells) {
        if (Strings.isNullOrEmpty(textValue)) {
            return;
        }

        codeableConcept.setText(textValue);

        addAudit("text", sourceCells);
    }

    /**
     * used when we want to populate the codeable concept from an existing one, with not audit information
     */
    public void addCodeableConceptNoAudit(CodeableConcept codeableConcept) {
        if (codeableConcept.hasCoding()) {
            for (Coding coding: codeableConcept.getCoding()) {
                String system = coding.getSystem();
                String code = coding.getCode();
                String display = coding.getDisplay();

                addCoding(system);
                setCodingCode(code);
                setCodingDisplay(display);
            }
        }

        if (codeableConcept.hasText()) {
            String text = codeableConcept.getText();
            setText(text);
        }
    }

    public CodeableConcept getCodeableConcept() {
        return codeableConcept;
    }
}
