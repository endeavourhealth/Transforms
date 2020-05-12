package org.endeavourhealth.transform.homerton;

import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.HasCodeableConceptI;

public class HomertonCodeableConceptHelper {

    public static final String AUDIT_ELEMENT_CODE_DESC = "Description";
    public static final String AUDIT_ELEMENT_CODE_DISPLAY = "Display";
    public static final String AUDIT_ELEMENT_CODE_MEANING = "Meaning";

    public static CodeableConceptBuilder applyCodeDescTxt(CsvCell codeCell, Long codeSet, HasCodeableConceptI resourceBuilder, CodeableConceptBuilder.Tag resourceBuilderTag, HomertonCsvHelper csvHelper) throws Exception {
        return applyCodeMeaningTxt(AUDIT_ELEMENT_CODE_DESC, codeCell, codeSet, resourceBuilder, resourceBuilderTag, csvHelper);
    }

    public static CodeableConceptBuilder applyCodeDisplayTxt(CsvCell codeCell, Long codeSet, HasCodeableConceptI resourceBuilder, CodeableConceptBuilder.Tag resourceBuilderTag, HomertonCsvHelper csvHelper) throws Exception {
        return applyCodeMeaningTxt(AUDIT_ELEMENT_CODE_DISPLAY, codeCell, codeSet, resourceBuilder, resourceBuilderTag, csvHelper);
    }

    public static CodeableConceptBuilder applyCodeMeaningTxt(CsvCell codeCell, Long codeSet, HasCodeableConceptI resourceBuilder, CodeableConceptBuilder.Tag resourceBuilderTag, HomertonCsvHelper csvHelper) throws Exception {
        return applyCodeMeaningTxt(AUDIT_ELEMENT_CODE_MEANING, codeCell, codeSet, resourceBuilder, resourceBuilderTag, csvHelper);
    }

    private static CodeableConceptBuilder applyCodeMeaningTxt(String elementToApply, CsvCell codeCell,
                                                              Long codeSet, HasCodeableConceptI resourceBuilder,
                                                              CodeableConceptBuilder.Tag resourceBuilderTag, HomertonCsvHelper csvHelper) throws Exception {
        if (codeCell == null || codeCell.isEmpty() || codeCell.getLong() == 0) {
            return null;
        }

        CernerCodeValueRef cernerCodeValueRef = csvHelper.lookupCodeRef(
                codeSet,
                codeCell.getString());

        if (cernerCodeValueRef == null) {
            return null;
        }

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
        codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_BARTS_CERNER_CODE_ID);
        codeableConceptBuilder.setCodingCode(codeCell.getString(), codeCell);
        codeableConceptBuilder.setCodingDisplay(term, termCsvCell);

        return codeableConceptBuilder;
    }

    private static CsvCell createCsvCell(CernerCodeValueRef codeMap, String fieldName, Object value) throws Exception {

        ResourceFieldMappingAudit audit = codeMap.getAudit();
        //audit may be null if the coding file was processed before the audit was added
        if (audit != null) {
            for (ResourceFieldMappingAudit.ResourceFieldMappingAuditRow rowAudit: audit.getAudits()) {
                for (ResourceFieldMappingAudit.ResourceFieldMappingAuditCol colAudit : rowAudit.getCols()) {
                    String field = colAudit.getField();
                    if (field.equals(fieldName)) {
                        short colIndex = colAudit.getCol();
                        int publishedFileId = rowAudit.getFileId();
                        return new CsvCell(publishedFileId, rowAudit.getRecord(), colIndex, value.toString(), null);
                    }
                }
            }
        }

        return null;
    }
}
