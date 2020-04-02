package org.endeavourhealth.transform.barts;

import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.core.database.dal.publisherTransform.models.ResourceFieldMappingAudit;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.HasCodeableConceptI;

public class BartsCodeableConceptHelper {

    public static final String CODE_VALUE = "Codeval";
    public static final String CODE_SET_NBR = "CodeSetNr";
    public static final String DISP_TXT = "DispTxt";
    public static final String DESC_TXT = "DescTxt";
    public static final String MEANING_TXT = "MeanTxt";
    public static final String ALIAS_TXT = "Alias";


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
        codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_BARTS_CERNER_CODE_ID);
        codeableConceptBuilder.setCodingCode(codeCell.getString(), codeCell);

        CernerCodeValueRef cernerCodeValueRef = csvHelper.lookupCodeRef(codeSet, codeCell);
        if (cernerCodeValueRef == null) {
            //if we fail to find the reference lookup, still carry the ID into the resource but also make it clear in the resource
            codeableConceptBuilder.setText("Failed to find in CVREF lookup");
            return codeableConceptBuilder;
        }

        //create a CSV cell to audit where the term came from, using the Audit object on the code reference entity
        CsvCell termCsvCell = createCsvCell(cernerCodeValueRef, elementToApply);
        String term = termCsvCell.getString();
        codeableConceptBuilder.setCodingDisplay(term, termCsvCell);

        //also set the term in the codeable concept text but with no audit, as we've already done it above
        codeableConceptBuilder.setText(term);

        return codeableConceptBuilder;
    }

    private static CsvCell createCsvCell(CernerCodeValueRef codeMap, String fieldName) throws Exception {

        String value = null;
        if (fieldName.equals(DESC_TXT)) {
            value = codeMap.getCodeDescTxt();

        } else if (fieldName.equals(DISP_TXT)) {
            value = codeMap.getCodeDispTxt();

        } else if (fieldName.equals(MEANING_TXT)) {
            value = codeMap.getCodeMeaningTxt();

        } else if (fieldName.equals(ALIAS_TXT)) {
            value = codeMap.getAliasNhsCdAlias();

        } else {
            throw new IllegalArgumentException("Unknown audit element " + fieldName);
        }

        ResourceFieldMappingAudit audit = codeMap.getAudit();
        //audit may be null if the coding file was processed before the audit was added
        if (audit != null) {
            for (ResourceFieldMappingAudit.ResourceFieldMappingAuditRow rowAudit: audit.getAudits()) {
                for (ResourceFieldMappingAudit.ResourceFieldMappingAuditCol colAudit : rowAudit.getCols()) {
                    String field = colAudit.getField();
                    if (field.equals(fieldName)) {
                        int colIndex = colAudit.getCol();
                        int publishedFileId = rowAudit.getFileId();
                        if (publishedFileId > 0) {
                            return new CsvCell(publishedFileId, rowAudit.getRecord(), colIndex, value.toString(), null);
                        } else if (rowAudit.getOldStyleAuditId() != null) {
                            //temproary, until all audits are converted over to new style
                            return CsvCell.factoryOldStyleAudit(rowAudit.getOldStyleAuditId(), colIndex, value.toString(), null);
                        } else {
                            throw new Exception("No published record ID in audit for CernerCodeValueRef " + codeMap.getCodeValueCd());
                        }
                    }
                }
            }
        }

        return CsvCell.factoryDummyWrapper(value);
    }

    public static CsvCell getCellDesc(BartsCsvHelper csvHelper, Long codeSet, CsvCell codeIdCell) throws Exception {
        CernerCodeValueRef mapping = csvHelper.lookupCodeRef(codeSet, codeIdCell);
        if (mapping != null) {
            return createCsvCell(mapping, DESC_TXT);
        } else {
            return null;
        }
    }

    public static CsvCell getCellDisp(BartsCsvHelper csvHelper, Long codeSet, CsvCell codeIdCell) throws Exception {
        CernerCodeValueRef mapping = csvHelper.lookupCodeRef(codeSet, codeIdCell);
        if (mapping != null) {
            return createCsvCell(mapping, DISP_TXT);
        } else {
            return null;
        }
    }

    public static CsvCell getCellMeaning(BartsCsvHelper csvHelper, Long codeSet, CsvCell codeIdCell) throws Exception {
        CernerCodeValueRef mapping = csvHelper.lookupCodeRef(codeSet, codeIdCell);
        if (mapping != null) {
            return createCsvCell(mapping, MEANING_TXT);
        } else {
            return null;
        }
    }

    public static CsvCell getCellAlias(BartsCsvHelper csvHelper, Long codeSet, CsvCell codeIdCell) throws Exception {
        CernerCodeValueRef mapping = csvHelper.lookupCodeRef(codeSet, codeIdCell);
        if (mapping != null) {
            return createCsvCell(mapping, ALIAS_TXT);
        } else {
            return null;
        }
    }


}
