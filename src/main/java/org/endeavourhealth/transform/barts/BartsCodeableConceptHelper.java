package org.endeavourhealth.transform.barts;

import org.endeavourhealth.common.fhir.FhirCodeUri;
import org.endeavourhealth.core.database.dal.publisherTransform.models.CernerCodeValueRef;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FhirResourceFiler;
import org.endeavourhealth.transform.common.resourceBuilders.CodeableConceptBuilder;
import org.endeavourhealth.transform.common.resourceBuilders.HasCodeableConceptI;

public class BartsCodeableConceptHelper {

    public static CodeableConceptBuilder applyCodeDescTxt(CsvCell codeCell, Long codeSet, HasCodeableConceptI resourceBuilder, String resourceBuilderTag, FhirResourceFiler fhirResourceFiler) throws Exception {
        if (codeCell.isEmpty() || codeCell.getLong() == 0) {
            return null;
        }

        CernerCodeValueRef cernerCodeValueRef = BartsCsvHelper.lookUpCernerCodeFromCodeSet(
                codeSet,
                codeCell.getLong(),
                fhirResourceFiler.getServiceId());


        //TODO - apply audit from code reference table
        String codeTerm = cernerCodeValueRef.getCodeDescTxt();

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(resourceBuilder, resourceBuilderTag);
        codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_CERNER_CODE_ID);
        codeableConceptBuilder.setCodingCode(codeCell.getString(), codeCell);
        codeableConceptBuilder.setCodingDisplay(codeTerm);

        return codeableConceptBuilder;
    }

    public static CodeableConceptBuilder applyCodeDisplayTxt(CsvCell codeCell, Long codeSet, HasCodeableConceptI resourceBuilder, String resourceBuilderTag, FhirResourceFiler fhirResourceFiler) throws Exception {
        if (codeCell.isEmpty() || codeCell.getLong() == 0) {
            return null;
        }

        CernerCodeValueRef cernerCodeValueRef = BartsCsvHelper.lookUpCernerCodeFromCodeSet(
                codeSet,
                codeCell.getLong(),
                fhirResourceFiler.getServiceId());


        //TODO - apply audit from code reference table
        String codeTerm = cernerCodeValueRef.getCodeDispTxt();

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(resourceBuilder, resourceBuilderTag);
        codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_CERNER_CODE_ID);
        codeableConceptBuilder.setCodingCode(codeCell.getString(), codeCell);
        codeableConceptBuilder.setCodingDisplay(codeTerm);

        return codeableConceptBuilder;
    }

    public static CodeableConceptBuilder applyCodeMeaningTxt(CsvCell codeCell, Long codeSet, HasCodeableConceptI resourceBuilder, String resourceBuilderTag, FhirResourceFiler fhirResourceFiler) throws Exception {
        if (codeCell.isEmpty() || codeCell.getLong() == 0) {
            return null;
        }

        CernerCodeValueRef cernerCodeValueRef = BartsCsvHelper.lookUpCernerCodeFromCodeSet(
                codeSet,
                codeCell.getLong(),
                fhirResourceFiler.getServiceId());


        //TODO - apply audit from code reference table
        String codeTerm = cernerCodeValueRef.getCodeMeaningTxt();

        CodeableConceptBuilder codeableConceptBuilder = new CodeableConceptBuilder(resourceBuilder, resourceBuilderTag);
        codeableConceptBuilder.addCoding(FhirCodeUri.CODE_SYSTEM_CERNER_CODE_ID);
        codeableConceptBuilder.setCodingCode(codeCell.getString(), codeCell);
        codeableConceptBuilder.setCodingDisplay(codeTerm);

        return codeableConceptBuilder;
    }
}
