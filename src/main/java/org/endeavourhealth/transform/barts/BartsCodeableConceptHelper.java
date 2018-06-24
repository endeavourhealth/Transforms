package org.endeavourhealth.transform.barts;

import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.HasCodeableConceptI;

import java.util.Map;

public class BartsCodeableConceptHelper {

    public static final String CODE_VALUE = "Codeval";
    public static final String CODE_SET_NBR = "CodeSetNr";
    public static final String DISP_TXT = "DispTxt";
    public static final String DESC_TXT = "DescTxt";
    public static final String MEANING_TXT = "MeanTxt";


    public static CodeableConceptBuilder applyCodeDescTxt(CsvCell codeCell, Long codeSet, HasCodeableConceptI resourceBuilder, CodeableConceptBuilder.Tag resourceBuilderTag, BartsCsvHelper csvHelper) throws Exception {
        return applyCodeMeaningTxt(DESC_TXT, codeCell, codeSet, resourceBuilder, resourceBuilderTag, csvHelper);
    }

    public static CodeableConceptBuilder applyCodeDisplayTxt(CsvCell codeCell, Long codeSet, HasCodeableConceptI resourceBuilder, CodeableConceptBuilder.Tag resourceBuilderTag, BartsCsvHelper csvHelper) throws Exception {
        return applyCodeMeaningTxt(DISP_TXT, codeCell, codeSet, resourceBuilder, resourceBuilderTag, csvHelper);
    }

    public static CodeableConceptBuilder applyCodeMeaningTxt(CsvCell codeCell, Long codeSet, HasCodeableConceptI resourceBuilder, CodeableConceptBuilder.Tag resourceBuilderTag, BartsCsvHelper csvHelper) throws Exception {
        return applyCodeMeaningTxt(MEANING_TXT, codeCell, codeSet, resourceBuilder, resourceBuilderTag, csvHelper);
    }

    private static CodeableConceptBuilder applyCodeMeaningTxt(String elementToApply, CsvCell codeCell,
                                                              Long codeSet, HasCodeableConceptI resourceBuilder,
                                                              CodeableConceptBuilder.Tag resourceBuilderTag, BartsCsvHelper csvHelper) throws Exception {

        if (codeCell == null
            || BartsCsvHelper.isEmptyOrIsZero(codeCell)) {
            return null;
        }

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(resourceBuilder, resourceBuilderTag);
        codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_CERNER_CODE_ID);
        codeableConceptBuilder.setCodingCode(codeCell.getString(), codeCell);

        CernerCodeValueRef cernerCodeValueRef = csvHelper.lookupCodeRef(codeSet, codeCell);
        if (cernerCodeValueRef == null) {
            //if we fail to find the reference lookup, still carry the ID into the resource but also make it clear in the resource
            codeableConceptBuilder.setText("Failed to find in CVREF lookup");
            return codeableConceptBuilder;
        }

        String term = null;
        if (elementToApply.equals(DESC_TXT)) {
            term = cernerCodeValueRef.getCodeDescTxt();

        } else if (elementToApply.equals(DISP_TXT)) {
            term = cernerCodeValueRef.getCodeDispTxt();

        } else if (elementToApply.equals(MEANING_TXT)) {
            term = cernerCodeValueRef.getCodeMeaningTxt();

        } else {
            throw new IllegalArgumentException("Unknown audit element " + elementToApply);
        }

        //create a CSV cell to audit where the term came from, using the Audit object on the code reference entity
        CsvCell termCsvCell = createCsvCell(cernerCodeValueRef, elementToApply, term);
        codeableConceptBuilder.setCodingDisplay(term, termCsvCell);

        //also set the term in the codeable concept text but with no audit, as we've already done it above
        codeableConceptBuilder.setText(term);

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
