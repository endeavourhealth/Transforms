package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class Pregnancy extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(Pregnancy.class);

    public Pregnancy(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                "yyyy-MM-dd", //different date format to standard
                BartsCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[] {
                "PERSON_ID",
                "MRN",
                "NHS",
                "MOTHER_NAME",
                "PREGNANCY_ID",
                "DELETE_IND",
                "MOTHER_DOB_DT_TM",
                "FIRST_ANTENATAL_ASSESSMENT_DT_TM",
                "MIDWIFE_TEAM_NAME",
                "GEST_AGE_PREG_START",
                "FINAL_EDD_DT_TM",
                "GEST_AGE_PREG_END",
                "GRAVIDA_NBR",
                "PARA_NBR",
                "HX_PREG_OUTCOME_SPONT_NBR",
                "HX_PREG_OUTCOME_VAG_SPONT_NBR",
                "HX_PREG_OUTCOME_VAG_BREECH_NBR",
                "HX_PREG_OUTCOME_ VAG_OTHER_INCL_DISTRUCTION_NBR",
                "HX_PREG_OUTCOME_VAG_FD_ASSIST_NBR",
                "HX_PREG_OUTCOME_VAG_VAC_ASSIST_NBR",
                "HX_PREG_OUTCOME_VBAC_NBR",
                "HX_PREG_OUTCOME_CS_EL_NBR",
                "HX_PREG_OUTCOME_CS_EM_NBR",
                "HX_PREG_OUTCOME_CS_EM_AFTER_FAILED_IN_NBR",
                "HX_PREG_OUTCOME_ECTOPIC_NBR",
                "HX_PREG_OUTCOME_ECTOPIC_SURG_NBR",
                "HX_PREG_OUTCOME_ECTOPIC_MED_NBR",
                "HX_PREG_OUTCOME_SPONT_ABORT_NBR",
                "HX_PREG_OUTCOME_SPONT_ABORT_DC_NBR",
                "HX_PREG_OUTCOME_THERAP_ABORT_MED_NBR",
                "HX_PREG_OUTCOME_ THERAP_ABORT_SURG_NBR",
                "HX_EPIDURAL_NBR",
                "PREV_PREG_PROBLEM_IND",
                "ACCOMMODATION_NM_ID",
                "ACCOMMODATION_DESC",
                "SUPPORT_STATUS_NM_ID",
                "SUPPORT_STATUS_DESC",
                "EMPLOY_STATUS_NM_ID",
                "EMPLOY_STATUS_DESC",
                "EDUCATION_NM_ID",
                "EDUCATION_DESC",
                "KNOWN_ SOC_ SERV_NM_ID",
                "KNOWN_ SOC_ SERV_DESC",
                "SMOKE_BOOKING_NM_ID",
                "SMOKE_BOOKING_DESC",
                "SMOKE_LIVES_WITH_NM_ID",
                "SMOKE_LIVES_WITH_DESC",
                "LAST_METHOD_CONTRACEPT_NM_ID",
                "LAST_METHOD_CONTRACEPT_DESC",
                "ALCOHOL_USE_NBR",
                "TOBAC_USE_NBR",
                "REC_SUB_USE_NM_ID",
                "REC_SUB_USE_DESC",
                "PARTNER_REC_SUB_USE_NM_ID",
                "PARTNER_REC_SUB_USE_DESC",
                "OCCUP_MOTHER",
                "OCCUP_PARTNER",
                "PARTNER_PAID_EMPLOY_BOOKING_NM_ID",
                "PARTNER_PAID_EMPLOY_BOOKING_DESC",
                "HT_BOOKING_CM",
                "WT_BOOKING_KG",
                "WT_EST_KG",
                "BMI_BOOKING_DESC",
                "FAMILY_ORIGIN_MOTHER_NM_ID",
                "FAMILY_ORIGIN_MOTHER_DESC",
                "FAMILY_ORIGIN_PARTNER_NM_ID",
                "FAMILY_ORIGIN_PARTNER_DESC",
                "PREG_TYPE_SCT_CD",
                "PREG_TYPE_DESC",
                "DOWNS_SERUM_SCREEN_NM_ID",
                "DOWNS_SERUM_SCREEN_DESC",
                "HEPB_ANTIGEN_SCREEN_NM_ID",
                "HEPB_ANTIGEN_SCREEN_DESC",
                "SYPHILIS_SCREEN_NM_ID",
                "SYPHILIS_SCREEN_DESC",
                "HIV_SCREEN_NM_ID",
                "HIV_SCREEN_DESC",
                "RUBELLA_IGG_SCREEN_NM_ID",
                "RUBELLA_IGG_SCREEN_DESC",
                "HAEMAGLOBINOPATHY_SCREEN_NM_ID",
                "HAEMAGLOBINOPATHY_SCREEN_DESC",
                "CHOOSING_TERMINATION_PREGNANCY_NM_ID",
                "CHOOSING_TERMINATION_PREGNANCY_DESC",
                "ECV_SCT_IND",
                "PLANNED_DEL_LOC_PREG_NM_ID",
                "PLANNED_DEL_LOC_PREG_DESC",
                "PLANNED_DEL_LOC_LAB_NM_ID",
                "PLANNED_DEL_LOC_LAB_DESC",
                "PLANNED_IOL_IND",
                "PLANNED_LSCS_IND",
                "PREG_REL_PROBLM_SCT_CD",
                "PREG_REL_PROBLM_DESC",
                "NONPREG_REL_PROBLM_SCT_CD",
                "NONPREG_REL_PROBLM_DESC",
                "ROM_METHOD_NM_ID",
                "ROM_METHOD_CD_DESC",
                "FULL_DIL_DT_TM",
                "PRES_FETUS_VE_NM_ID",
                "PRES_FETUS_VE_DESC",
                "LAB_STAGE1_TOTAL",
                "LAB_STAGE2_TOTAL",
                "LAB_STAGE3_TOTAL",
                "LAB_TOTAL",
                "DEL_LOC_CHANGE_REASON_PREG_NM_ID",
                "DEL_LOC_CHANGE_REASON_PREG_DESC",
                "GEST_DEATH_CONF_DT_TM",
                "UNBOOKED_NM_ID",
                "UNBOOKED_DESC",
                "ROM_DT_TM",
                "LAB_ONSET_DT_TM",
                "LAB_ONSET_METHOD_NM_ID",
                "LAB_ONSET_METHOD_DESC",
                "AUGMENTATION_NM_ID",
                "AUGMENTATION_DESC",
                "LAB_SUPPORT_NM_ID",
                "LAB_SUPPORT_DESC",
                "EPISIOTOMY_NM_ID",
                "EPISIOTOMY_DESC",
                "PERINEAL_TRAUMA_NM_ID",
                "PERINEAL_TRAUMA_DESC",
                "ANALGESIA_DEL_NM_ID",
                "ANALGESIA_DEL_DESC",
                "ANALGESIA_LAB_NM_ID",
                "ANALGESIA_LAB_DESC",
                "ANAESTHESIA_LAB_NM_ID",
                "ANAESTHESIA_LAB_DESC",
                "ANAESTHESIA_DEL_NM_ID",
                "ANAESTHESIA_DEL_DESC",
                "ANAESTHESIA_PN_NM_ID",
                "ANAESTHESIA_PN_DESC",
                "BLOOD_PATCH_IND",
                "BIRTHING_POOL_USE_NM_ID",
                "BIRTHING_POOL_USE_DESC",
                "SMOKING_STATUS_DEL_NM_ID",
                "SMOKING_STATUS_DEL_DESC",
                "DEL_LOC_CHANGE_REASON_LAB_NM_ID",
                "DEL_LOC_CHANGE_REASON_LAB_DESC",
                "TOTAL_BLOOD_LOSS",
                "PLACENTA_APPEARANCE_NM_ID",
                "PLACENTA_APPEARANCE_DESC",
                "PLACENTA_MAN_REMOVAL_IND",
                "DEATH_LAB_WD_DT_TM"
        };
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}

/*public class Pregnancy extends AbstractFixedParser {

    public Pregnancy(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, EmisCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD, EmisCsvToFhirTransformer.TIME_FORMAT);
    }

    public String getLocalPatientId() {
        return super.getString("MRN");
    }
    public String getDOB() {
        return super.getString("DOB");
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    @Override
    protected boolean skipFirstRow() {
        return true;
    }

    @Override
    protected List<FixedParserField> getFieldList(String version) {

        List<FixedParserField> ret = new ArrayList<>();

        ret.add(new FixedParserField("CDSVersion",             1, 6));
        ret.add(new FixedParserField("CDSRecordType",          7, 3));
        ret.add(new FixedParserField("CDSReplacementgroup",    10, 3));
        ret.add(new FixedParserField("MRN",    284, 10));
        ret.add(new FixedParserField("DOB",    321, 8));
        
        return ret;
    }
}*/
