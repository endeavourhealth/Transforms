package org.endeavourhealth.transform.homerton.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.homerton.HomertonCsvToFhirTransformer;

import java.util.UUID;

public class DiagnosisTable extends AbstractCsvParser {

    public DiagnosisTable(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, HomertonCsvToFhirTransformer.CSV_FORMAT, HomertonCsvToFhirTransformer.DATE_FORMAT, HomertonCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {

        return new String[]{
                "DIAGNOSIS_ID",
                "UPDT_CNT",
                "UPDT_DT_TM",
                "UPDT_ID",
                "UPDT_TASK",
                "UPDT_APPLCTX",
                "ACTIVE_IND",
                "ACTIVE_STATUS_CD",
                "ACTIVE_STATUS_DT_TM",
                "ACTIVE_STATUS_PRSNL_ID",
                "BEG_EFFECTIVE_DT_TM",
                "END_EFFECTIVE_DT_TM",
                "CONTRIBUTOR_SYSTEM_CD",
                "PERSON_ID",
                "ENCNTR_ID",
                "NOMENCLATURE_ID",
                "DIAG_DT_TM",
                "DIAG_TYPE_CD",
                "DIAGNOSTIC_CATEGORY_CD",
                "DIAG_PRIORITY",
                "DIAG_PRSNL_ID",
                "DIAG_PRSNL_NAME",
                "DIAG_CLASS_CD",
                "CONFID_LEVEL_CD",
                "ATTESTATION_DT_TM",
                "REFERENCE_NBR",
                "SEG_UNIQUE_KEY",
                "DIAG_FTDESC",
                "MOD_NOMENCLATURE_ID",
                "SVC_CAT_HIST_ID",
                "DIAG_NOTE",
                "CONDITIONAL_QUAL_CD",
                "CLINICAL_SERVICE_CD",
                "CONFIRMATION_STATUS_CD",
                "CLASSIFICATION_CD",
                "SEVERITY_CLASS_CD",
                "CERTAINTY_CD",
                "PROBABILITY",
                "DIAGNOSIS_DISPLAY",
                "SEVERITY_FTDESC",
                "LONG_BLOB_ID",
                "RANKING_CD",
                "SEVERITY_CD",
                "DIAGNOSIS_GROUP",
                "CLINICAL_DIAG_PRIORITY",
                "ENCNTR_SLICE_ID",
                "PRESENT_ON_ADMIT_CD",
                "ORIGINATING_NOMENCLATURE_ID",
                "HAC_IND",
                "LATERALITY_CD",
                "LAST_UPDATED",
                "IMPORT_ID",
                "HASH",
                "diag_type",
                "diag_class",
                "ranking",
                "confirmation_status",
                "classification",
                "severity_class",
                "laterality",
                "source_identifier",
                "source_vocabulary_cd",
                "source_vocabulary",
                "nom_ver_grp_id",
                "primary_vterm_ind",
                "nomenclature_description",
                "principle_type_cd",
                "principle_type"
        };

    }


    public CsvCell getDiagnosisID()  {
        return super.getCell("DIAGNOSIS_ID");
    }

    public CsvCell getActiveIndicator() {
        return super.getCell("ACTIVE_IND");
    }

    public CsvCell getPersonId() {
        return super.getCell("PERSON_ID");
    }

    public CsvCell getEncounterID() {
        return super.getCell("ENCNTR_ID");
    }

    public CsvCell getDiagnosisDateTime() {
        return super.getCell("DIAG_DT_TM");
    }

    public CsvCell getEncounterSliceID() {
        return super.getCell("ENCNTR_SLICE_ID");
    }

    public CsvCell getPersonnelId() {
        return super.getCell("DIAG_PRSNL_ID");
    }

    public CsvCell getConceptCode() {
        return super.getCell("source_identifier");
    }

    public CsvCell getConceptCodeType() {
        return super.getCell("source_vocabulary");
    }

    public CsvCell getDiagnosisDisplay() {
        return super.getCell("DIAGNOSIS_DISPLAY");
    }

    public CsvCell getDiagnosisType() {
        return super.getCell("principle_type");
    }

    public CsvCell getDiagnosisTypeCode() {
        return super.getCell("principle_type_cd");
    }

    public CsvCell getConfirmation() {
        return super.getCell("confirmation_status");
    }

    @Override
    protected String getFileTypeDescription() {
        return "Homerton Diagnosis file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

}
