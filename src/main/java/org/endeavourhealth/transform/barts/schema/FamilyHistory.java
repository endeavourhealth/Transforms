package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;

import java.util.UUID;

public class FamilyHistory extends AbstractCsvParser {

    public FamilyHistory(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                BartsCsvToFhirTransformer.DATE_FORMAT,
                BartsCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[] {
                "FHX_ACTIVITY_KEY",
                "HEALTH_SYSTEM_ID",
                "HEALTH_SYSTEM_SOURCE_ID",
                "FHX_ACTIVITY_ID",
                "FHX_ACTIVITY_GROUP_ID",
                "PERSON_ID",
                "RELATED_PERSON_ID",
                "RELATED_PERSON_RELTN_REF",
                "FHX_VALUE_FLG",
                "ONSET_AGE",
                "ONSET_AGE_PREC_REF",
                "ONSET_AGE_UNIT_REF",
                "ACTIVITY_ORG",
                "COURSE_REF",
                "LIFE_CYCLE_STATUS_REF",
                "ACTIVITY_NOMEN",
                "SEVERITY_REF",
                "TYPE_MEAN",
                "SRC_BEG_EFFECT_DT_TM",
                "SRC_BEG_EFFECT_TM_VLD_FLG",
                "SRC_BEG_EFFECT_TM_ZN",
                "SRC_END_EFFECT_DT_TM",
                "SRC_END_EFFECT_TM_VLD_FLG",
                "SRC_END_EFFECT_TM_ZN",
                "ACTIVE_IND",
                "TRAN_PRSNL_HSS_ID",
                "CREATE_PRSNL",
                "CREATE_TRAN_PRSNL",
                "CREATE_DT_TM",
                "CREATE_TM_VLD_FLG",
                "CREATE_TM_ZN",
                "INACTIVATE_PRSNL",
                "INACTIVATE_TRAN_PRSNL",
                "INACTIVATE_DT_TM",
                "INACTIVATE_TM_VLD_FLG",
                "INACTIVATE_TM_ZN",
                "FIRST_REVIEW_PRSNL",
                "FIRST_REVIEW_TRAN_PRSNL",
                "FIRST_REVIEW_DT_TM",
                "FIRST_REVIEW_TM_VLD_FLG",
                "FIRST_REVIEW_TM_ZN",
                "LAST_REVIEW_PRSNL",
                "LAST_REVIEW_TRAN_PRSNL",
                "LAST_REVIEW_DT_TM",
                "LAST_REVIEW_TM_VLD_FLG",
                "LAST_REVIEW_TM_ZN",
                "SECURITY_KEY",
                "SECURITY_PROCESS_DT_TM",
                "DUPLICATE_FLG",
                "ORPHAN_FLG",
                "ERROR_IND",
                "FIRST_PROCESS_DT_TM",
                "LAST_PROCESS_DT_TM",
                "TOTAL_UPDATES",
                "UPDT_DT_TM",
                "UPDT_TASK",
                "UPDT_USER",
                "SOURCE_FLG",
                "EXTRACT_DT_TM",
                "PARTITION_DT_TM",
                "Record_Updated_Dt"
        };
    }

    @Override
    protected String getFileTypeDescription() {
        return "Barts family history file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
