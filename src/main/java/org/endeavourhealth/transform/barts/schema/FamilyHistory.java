package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;

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


    public CsvCell getFhxActivityKey()  { return super.getCell( "FHX_ACTIVITY_KEY");}
    public CsvCell getHealthSystemId()  { return super.getCell( "HEALTH_SYSTEM_ID");}
    public CsvCell getHealthSystemSourceId()  { return super.getCell( "HEALTH_SYSTEM_SOURCE_ID");}
    public CsvCell getFhxActivityId()  { return super.getCell( "FHX_ACTIVITY_ID");}
    public CsvCell getFhxActivityGroupId()  { return super.getCell( "FHX_ACTIVITY_GROUP_ID");}
    public CsvCell getPersonId()  { return super.getCell( "PERSON_ID");}
    public CsvCell getRelatedPersonId()  { return super.getCell( "RELATED_PERSON_ID");}
    public CsvCell getRelatedPersonReltnRef()  { return super.getCell( "RELATED_PERSON_RELTN_REF");}
    public CsvCell getFhxvalueFlag()  { return super.getCell( "FHX_VALUE_FLG");}
    public CsvCell getOnsetAge()  { return super.getCell( "ONSET_AGE");}
    public CsvCell getOnsetAgrePrecRef()  { return super.getCell( "ONSET_AGE_PREC_REF");}
    public CsvCell getOnsetAgeUnitRef()  { return super.getCell( "ONSET_AGE_UNIT_REF");}
    public CsvCell getActivityOrg()  { return super.getCell( "ACTIVITY_ORG");}
    public CsvCell getCourseRef()  { return super.getCell( "COURSE_REF");}
    public CsvCell getLifeCycleStatusRef()  { return super.getCell( "LIFE_CYCLE_STATUS_REF");}
    public CsvCell getActivityNomen()  { return super.getCell( "ACTIVITY_NOMEN");}
    public CsvCell getSeverityRef()  { return super.getCell( "SEVERITY_REF");}
    public CsvCell getTypeMean()  { return super.getCell( "TYPE_MEAN");}
    public CsvCell getSrcBegEffectDtTm()  { return super.getCell( "SRC_BEG_EFFECT_DT_TM");}
    public CsvCell getSrcBegEffectTmVldFlg() { return super.getCell( "SRC_BEG_EFFECT_TM_VLD_FLG");}
    public CsvCell getSrcBegEffectTmZn()  { return super.getCell( "SRC_BEG_EFFECT_TM_ZN");}
    public CsvCell getSrcEndEffectDtTm()  { return super.getCell( "SRC_END_EFFECT_DT_TM");}
    public CsvCell getSrcEndEffectTmVldFlg()  { return super.getCell( "SRC_END_EFFECT_TM_VLD_FLG");}
    public CsvCell getSrcEndEffectTmZn() { return super.getCell( "SRC_END_EFFECT_TM_ZN");}
    public CsvCell getActiveInd()  { return super.getCell( "ACTIVE_IND");}
    public CsvCell getTranPrsnlHssId()  { return super.getCell( "TRAN_PRSNL_HSS_ID");}
    public CsvCell getCreatePrsnl()  { return super.getCell( "CREATE_PRSNL");}
    public CsvCell getCreateTransPrsnl()  { return super.getCell( "CREATE_TRAN_PRSNL");}
    public CsvCell getCreateDtTm()  { return super.getCell( "CREATE_DT_TM");}
    public CsvCell getCreateTmVldFlg()  { return super.getCell( "CREATE_TM_VLD_FLG");}
    public CsvCell getCreateTmZn()  { return super.getCell( "CREATE_TM_ZN");}
    public CsvCell getInactivatePrsnl()  { return super.getCell( "INACTIVATE_PRSNL");}
    public CsvCell getInactivateTranPrsl()  { return super.getCell( "INACTIVATE_TRAN_PRSNL");}
    public CsvCell getInactivateDtTm()  { return super.getCell( "INACTIVATE_DT_TM");}
    public CsvCell getInactivateTmVldFlg()  { return super.getCell( "INACTIVATE_TM_VLD_FLG");}
    public CsvCell getInactivateTmZn()  { return super.getCell( "INACTIVATE_TM_ZN");}
    public CsvCell getFirstReviewPrsnl()  { return super.getCell( "FIRST_REVIEW_PRSNL");}
    public CsvCell getFirstReviewTranPrsnl()  { return super.getCell( "FIRST_REVIEW_TRAN_PRSNL");}
    public CsvCell getFirstReviewDtTM()  { return super.getCell( "FIRST_REVIEW_DT_TM");}
    public CsvCell getFirstReviewTmVldFlg()  { return super.getCell( "FIRST_REVIEW_TM_VLD_FLG");}
    public CsvCell getFirstReviewTmZn()  { return super.getCell( "FIRST_REVIEW_TM_ZN");}
    public CsvCell getLastReviewPrsnl()  { return super.getCell( "LAST_REVIEW_PRSNL");}
    public CsvCell getLastReviewTranPrsnl()  { return super.getCell( "LAST_REVIEW_TRAN_PRSNL");}
    public CsvCell getLastReviewDtTM()  { return super.getCell( "LAST_REVIEW_DT_TM");}
    public CsvCell getLastReviewTmVldFlg()  { return super.getCell( "LAST_REVIEW_TM_VLD_FLG");}
    public CsvCell getLastReviewTmZn()  { return super.getCell( "LAST_REVIEW_TM_ZN");}
    public CsvCell getSecurityKey()  { return super.getCell( "SECURITY_KEY");}
    public CsvCell getSecurityProcessDtTm()  { return super.getCell( "SECURITY_PROCESS_DT_TM");}
    public CsvCell getDuplicateFlag()  { return super.getCell( "DUPLICATE_FLG");}
    public CsvCell getOrphanFlag()  { return super.getCell( "ORPHAN_FLG");}
    public CsvCell getErrorInd()  { return super.getCell( "ERROR_IND");}
    public CsvCell getFirstProcessDtTm()  { return super.getCell( "FIRST_PROCESS_DT_TM");}
    public CsvCell getLastProcessDtTm()  { return super.getCell( "LAST_PROCESS_DT_TM");}
    public CsvCell getTotalUpdates()  { return super.getCell( "TOTAL_UPDATES");}
    public CsvCell getUpdtDtTm()  { return super.getCell( "UPDT_DT_TM");}
    public CsvCell getUpdtTask()  { return super.getCell( "UPDT_TASK");}
    public CsvCell getUpdtUser()  { return super.getCell( "UPDT_USER");}
    public CsvCell getSourceFlag()  { return super.getCell( "SOURCE_FLG");}
    public CsvCell getExtractDtTm()  { return super.getCell( "EXTRACT_DT_TM");}
    public CsvCell getPartitionDtTm()  { return super.getCell( "PARTITION_DT_TM");}
    public CsvCell getRecordUpdatedDt()  { return super.getCell( "Record_Updated_Dt");}

    @Override
    protected String getFileTypeDescription() {
        return "Barts family history file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
