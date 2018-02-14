package org.endeavourhealth.transform.common.resourceBuilders;

import com.google.common.base.Strings;
import org.endeavourhealth.common.fhir.CodingHelper;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.hl7.fhir.instance.model.CodeableConcept;
import org.hl7.fhir.instance.model.Coding;

public class CodeableConceptBuilder {

    private HasCodeableConceptI parentBuilder = null;
    private String codeableConceptTag = null;

    public CodeableConceptBuilder(HasCodeableConceptI parentBuilder, String codeableConceptTag) {
        this.parentBuilder = parentBuilder;
        this.codeableConceptTag = codeableConceptTag;
    }

    public void addCoding(String systemUrl, CsvCell... sourceCells) {
        CodeableConcept codeableConcept = this.parentBuilder.getOrCreateCodeableConcept(codeableConceptTag);
        Coding coding = CodingHelper.createCoding(systemUrl, null, null);
        codeableConcept.addCoding(coding);

        addCodingAudit(codeableConcept, coding, "system", sourceCells);
    }

    public void setCodingCode(String code, CsvCell... sourceCells) {
        if (Strings.isNullOrEmpty(code)) {
            return;
        }
        CodeableConcept codeableConcept = this.parentBuilder.getOrCreateCodeableConcept(codeableConceptTag);
        Coding coding = getLastCoding(codeableConcept);
        coding.setCode(code);

        addCodingAudit(codeableConcept, coding, "code", sourceCells);
    }

    public void setCodingDisplay(String display, CsvCell... sourceCells) {
        if (Strings.isNullOrEmpty(display)) {
            return;
        }
        CodeableConcept codeableConcept = this.parentBuilder.getOrCreateCodeableConcept(codeableConceptTag);
        Coding coding = getLastCoding(codeableConcept);
        coding.setDisplay(display);

        addCodingAudit(codeableConcept, coding, "display", sourceCells);
    }

    private Coding getLastCoding(CodeableConcept codeableConcept) {
        if (!codeableConcept.hasCoding()) {
            throw new IllegalArgumentException("Must call addCoding before setting code or disply");
        }
        int index = codeableConcept.getCoding().size()-1;
        return codeableConcept.getCoding().get(index);
    }

    private void addCodingAudit(CodeableConcept codeableConcept, Coding coding, String element, CsvCell... sourceCells) {
        ResourceFieldMappingAudit audit = this.parentBuilder.getAuditWrapper();
        int index = codeableConcept.getCoding().indexOf(coding);

        String jsonField = this.parentBuilder.getCodeableConceptJsonPath(codeableConceptTag) + ".coding[" + index + "]." + element;

        for (CsvCell csvCell: sourceCells) {
            audit.auditValue(csvCell.getRowAuditId(), csvCell.getColIndex(), jsonField);
        }
    }

    public void setText(String textValue, CsvCell... sourceCells) {
        if (Strings.isNullOrEmpty(textValue)) {
            return;
        }

        CodeableConcept codeableConcept = this.parentBuilder.getOrCreateCodeableConcept(codeableConceptTag);
        codeableConcept.setText(textValue);

        String jsonField = this.parentBuilder.getCodeableConceptJsonPath(codeableConceptTag) + ".text";
        ResourceFieldMappingAudit audit = this.parentBuilder.getAuditWrapper();
        for (CsvCell csvCell: sourceCells) {
            audit.auditValue(csvCell.getRowAuditId(), csvCell.getColIndex(), jsonField);
        }
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
}
