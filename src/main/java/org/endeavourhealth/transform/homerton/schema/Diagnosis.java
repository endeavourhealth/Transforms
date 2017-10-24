package org.endeavourhealth.transform.homerton.schema;

import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;
import org.endeavourhealth.transform.emis.csv.schema.AbstractCsvParser;

import java.io.File;

public class Diagnosis extends AbstractCsvParser {

    public Diagnosis(String version, File f, boolean openParser) throws Exception {
        super(version, f, openParser, EmisCsvToFhirTransformer.CSV_FORMAT, EmisCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD, EmisCsvToFhirTransformer.TIME_FORMAT);
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

    public Long getDiagnosisId()  {
        return super.getLong("DIAGNOSIS_ID");
    }
    public boolean getActiveIndicator() {
        String val = super.getString("ACTIVE_IND");
        if(val.compareTo("1")==0)
        {
            return true;
        } else
        {
            return false;
        }

    }

    public String getPersonId() {
        return super.getString("PERSON_ID");
    }
    public String getEncounterId() {
        return super.getString("ENCNTR_ID");
    }

    public String getCNN() {
        return super.getString("CNN");
    }



}
