package org.endeavourhealth.transform.barts;

import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.HasCodeableConceptI;

import java.util.Map;

public class BartsCodeableConceptHelper {

    public static final String AUDIT_ELEMENT_CODE_DESC = "Description";
    public static final String AUDIT_ELEMENT_CODE_DISPLAY = "Display";
    public static final String AUDIT_ELEMENT_CODE_MEANING = "Meaning";

    public static CodeableConceptBuilder applyCodeDescTxt(CsvCell codeCell, Long codeSet, HasCodeableConceptI resourceBuilder, String resourceBuilderTag, BartsCsvHelper csvHelper) throws Exception {
        return applyCodeMeaningTxt(AUDIT_ELEMENT_CODE_DESC, codeCell, codeSet, resourceBuilder, resourceBuilderTag, csvHelper);
    }

    public static CodeableConceptBuilder applyCodeDisplayTxt(CsvCell codeCell, Long codeSet, HasCodeableConceptI resourceBuilder, String resourceBuilderTag, BartsCsvHelper csvHelper) throws Exception {
        return applyCodeMeaningTxt(AUDIT_ELEMENT_CODE_DISPLAY, codeCell, codeSet, resourceBuilder, resourceBuilderTag, csvHelper);
    }

    public static CodeableConceptBuilder applyCodeMeaningTxt(CsvCell codeCell, Long codeSet, HasCodeableConceptI resourceBuilder, String resourceBuilderTag, BartsCsvHelper csvHelper) throws Exception {
        return applyCodeMeaningTxt(AUDIT_ELEMENT_CODE_MEANING, codeCell, codeSet, resourceBuilder, resourceBuilderTag, csvHelper);
    }

    private static CodeableConceptBuilder applyCodeMeaningTxt(String elementToApply, CsvCell codeCell,
                                                              Long codeSet, HasCodeableConceptI resourceBuilder,
                                                              String resourceBuilderTag, BartsCsvHelper csvHelper) throws Exception {
        if (codeCell.isEmpty() || codeCell.getLong() == 0) {
            return null;
        }

        CernerCodeValueRef cernerCodeValueRef = csvHelper.lookUpCernerCodeFromCodeSet(
                codeSet,
                codeCell.getLong());

        //TODO - apply audit from code reference table
        String term = null;
        if (elementToApply.equals(AUDIT_ELEMENT_CODE_DESC)) {
            term = cernerCodeValueRef.getCodeDescTxt();

        } else if (elementToApply.equals(AUDIT_ELEMENT_CODE_DISPLAY)) {
            term = cernerCodeValueRef.getCodeDispTxt();

        } else if (elementToApply.equals(AUDIT_ELEMENT_CODE_MEANING)) {
            term = cernerCodeValueRef.getCodeMeaningTxt();

        } else {
            throw new IllegalArgumentException("Unknown audit element " + elementToApply);
        }

        //create a CSV cell to audit where the term came from, using the Audit object on the code reference entity
        CsvCell termCsvCell = createCsvCell(cernerCodeValueRef, elementToApply, term);

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(resourceBuilder, resourceBuilderTag);
        codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_CERNER_CODE_ID);
        codeableConceptBuilder.setCodingCode(codeCell.getString(), codeCell);
        codeableConceptBuilder.setCodingDisplay(term, termCsvCell);

        return codeableConceptBuilder;
    }

    private static CsvCell createCsvCell(CernerCodeValueRef codeMap, String fieldName, Object value) {

        ResourceFieldMappingAudit audit = codeMap.getAudit();
        //audit may be null if the coding file was processed before the audit was added
        if (audit != null) {
            Map<Long, ResourceFieldMappingAudit.ResourceFieldMappingAuditRow> auditMap = audit.getAudits();
            for (Long key : auditMap.keySet()) {
                ResourceFieldMappingAudit.ResourceFieldMappingAuditRow rowAudit = auditMap.get(key);
                for (ResourceFieldMappingAudit.ResourceFieldMappingAuditCol colAudit : rowAudit.getCols()) {
                    String field = colAudit.getField();
                    if (field.equals(fieldName)) {
                        int colIndex = colAudit.getCol();
                        long rowAuditId = rowAudit.getAuditId();

                        return new CsvCell(rowAuditId, colIndex, value.toString(), null);
                    }
                }
            }
        }

        return null;
    }
}
