package org.endeavourhealth.transform.homerton.schema;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.homerton.HomertonCsvToFhirTransformer;

import java.util.UUID;

public class Procedure extends AbstractCsvParser {

    public Procedure(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, HomertonCsvToFhirTransformer.CSV_FORMAT, HomertonCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD, HomertonCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {

            return new String[] {
                    "PROCEDURE_ID",
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
                    "ENCNTR_ID",
                    "NOMENCLATURE_ID",
                    "PROC_DT_TM",
                    "PROC_PRIORITY",
                    "PROC_FUNC_TYPE_CD",
                    "PROC_MINUTES",
                    "CONSENT_CD",
                    "DIAG_NOMENCLATURE_ID",
                    "REFERENCE_NBR",
                    "SEG_UNIQUE_KEY",
                    "MOD_NOMENCLATURE_ID",
                    "ANESTHESIA_CD",
                    "ANESTHESIA_MINUTES",
                    "TISSUE_TYPE_CD",
                    "SVC_CAT_HIST_ID",
                    "PROC_LOC_CD",
                    "PROC_LOC_FT_IND",
                    "PROC_FT_LOC",
                    "PROC_FT_DT_TM_IND",
                    "PROC_FT_TIME_FRAME",
                    "COMMENT_IND",
                    "LONG_TEXT_ID",
                    "PROC_FTDESC",
                    "PROCEDURE_NOTE",
                    "GENERIC_VAL_CD",
                    "RANKING_CD",
                    "CLINICAL_SERVICE_CD",
                    "DGVP_IND",
                    "ENCNTR_SLICE_ID",
                    "UNITS_OF_SERVICE",
                    "PROC_DT_TM_PREC_FLAG",
                    "PROC_TYPE_FLAG",
                    "SUPPRESS_NARRATIVE_IND",
                    "PROC_DT_TM_PREC_CD",
                    "LATERALITY_CD",
                    "LAST_UPDATED",
                    "IMPORT_ID",
                    "HASH",
                    "proc_loc",
                    "ranking",
                    "clinical_service",
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

    @Override
    protected String getFileTypeDescription() {
        return "Homerton procedure file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getProcedureId() {
        return super.getCell("PROCEDURE_ID");
    }
    public CsvCell getPatientDimID() {
        return super.getCell("PatientDimID");
    }
    public CsvCell getPersonId() {
        return super.getCell("PersonId");
    }
    public CsvCell getCNN() {
        return super.getCell("CNN");
    }
    
    /*public String getProcedureId() {
        return super.getString("PROCEDURE_ID");
    }
    public String getPatientDimID() {
        return super.getString("PatientDimID");
    }
    public String getPersonId() {
        return super.getString("PersonId");
    }
    public String getCNN() {
        return super.getString("CNN");
    }*/

}
