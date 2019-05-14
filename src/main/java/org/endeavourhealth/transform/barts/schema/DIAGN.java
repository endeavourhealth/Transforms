package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class DIAGN extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(DIAGN.class);

    public DIAGN(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                null, null); //all Barts date parsing for Power Insight content should use BartsCsvHelper.parseDate(..)
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "#DIAGNOSIS_ID",
                "ACTIVE_IND",
                "EXTRACT_DT_TM",
                "ENCNTR_ID",
                "ENCNTR_SLICE_ID",
                "DIAGNOSIS_SEQ_NBR",
                "NOMENCLATURE_ID",
                "DIAGNOSIS_DT_TM",
                "DIAG_HCP_PRSNL_ID",
                "DIAGNOSIS_TYPE_CD",
                "CONCEPT_CKI_IDENT",
                "DIAGNOSIS_TXT", //is this used
                "DIAGNOSIS_ENTRY_SEQ_NBR",
        };
    }

    public CsvCell getDiagnosisID() {
        return super.getCell("#DIAGNOSIS_ID");
    }

    public CsvCell getActiveIndicator() {
        return super.getCell("ACTIVE_IND");
    }

    public CsvCell getExtractDateTime() {
        return super.getCell("EXTRACT_DT_TM");
    }

    public CsvCell getEncounterId() {
        return super.getCell("ENCNTR_ID");
    }

    public CsvCell getEncounterSliceID() {
        return super.getCell("ENCNTR_SLICE_ID");
    }

    public CsvCell getSequenceNumber() {
        return super.getCell("DIAGNOSIS_SEQ_NBR");
    }

    public CsvCell getNomenclatureID() {
        return super.getCell("NOMENCLATURE_ID");
    }

    public CsvCell getDiagnosisDateTime() {
        return super.getCell("DIAGNOSIS_DT_TM");
    }

    public CsvCell getPersonnelId() {
        return super.getCell("DIAG_HCP_PRSNL_ID");
    }

    public CsvCell getDiagnosisTypeCode() {
        return super.getCell("DIAGNOSIS_TYPE_CD");
    }

    public CsvCell getConceptCodeIdentifier() {
        return super.getCell("CONCEPT_CKI_IDENT");
    }

    public CsvCell getDiagnosicFreeText() {
        return super.getCell("DIAGNOSIS_TXT");
    }

    public CsvCell getProcedureCodeSequenceEntryOrder() {
        return super.getCell("DIAGNOSIS_ENTRY_SEQ_NBR");
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}