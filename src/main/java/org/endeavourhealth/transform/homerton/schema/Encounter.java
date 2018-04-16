package org.endeavourhealth.transform.homerton.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class Encounter extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(Encounter.class);

    public Encounter(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                BartsCsvToFhirTransformer.DATE_FORMAT,
                BartsCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "ENCNTR_ID",
                "PERSON_ID",
                "UPDT_CNT",
                "UPDT_DT_TM",
                "UPDT_ID",
                "UPDT_TASK",
                "UPDT_APPLCTX",
                "ACTIVE_IND",
                "ACTIVE_STATUS_CD",
                "ACTIVE_STATUS_DT_TM",
                "ACTIVE_STATUS_PRSNL_ID",
                "CREATE_DT_TM",
                "CREATE_PRSNL_ID",
                "BEG_EFFECTIVE_DT_TM",
                "END_EFFECTIVE_DT_TM",
                "ENCNTR_CLASS_CD",
                "ENCNTR_TYPE_CD",
                "ENCNTR_TYPE_CLASS_CD",
                "ENCNTR_STATUS_CD",
                "PRE_REG_DT_TM",
                "PRE_REG_PRSNL_ID",
                "REG_DT_TM",
                "REG_PRSNL_ID",
                "EST_ARRIVE_DT_TM",
                "EST_DEPART_DT_TM",
                "ARRIVE_DT_TM",
                "DEPART_DT_TM",
                "ADMIT_TYPE_CD",
                "ADMIT_SRC_CD",
                "ADMIT_MODE_CD",
                "ADMIT_WITH_MEDICATION_CD",
                "REFERRING_COMMENT",
                "DISCH_DISPOSITION_CD",
                "DISCH_TO_LOCTN_CD",
                "PREADMIT_NBR",
                "PREADMIT_TESTING_CD",
                "READMIT_CD",
                "ACCOMMODATION_CD",
                "ACCOMMODATION_REQUEST_CD",
                "ALT_RESULT_DEST_CD",
                "AMBULATORY_COND_CD",
                "COURTESY_CD",
                "DIET_TYPE_CD",
                "ISOLATION_CD",
                "MED_SERVICE_CD",
                "RESULT_DEST_CD",
                "CONFID_LEVEL_CD",
                "VIP_CD",
                "NAME_LAST_KEY",
                "NAME_FIRST_KEY",
                "NAME_FULL_FORMATTED",
                "NAME_LAST",
                "NAME_FIRST",
                "NAME_PHONETIC",
                "SEX_CD",
                "BIRTH_DT_CD",
                "BIRTH_DT_TM",
                "SPECIES_CD",
                "DATA_STATUS_CD",
                "DATA_STATUS_DT_TM",
                "DATA_STATUS_PRSNL_ID",
                "CONTRIBUTOR_SYSTEM_CD",
                "LOCATION_CD",
                "LOC_FACILITY_CD",
                "LOC_BUILDING_CD",
                "LOC_NURSE_UNIT_CD",
                "LOC_ROOM_CD",
                "LOC_BED_CD",
                "DISCH_DT_TM",
                "GUARANTOR_TYPE_CD",
                "LOC_TEMP_CD",
                "ORGANIZATION_ID",
                "REASON_FOR_VISIT",
                "ENCNTR_FINANCIAL_ID",
                "NAME_FIRST_SYNONYM_ID",
                "FINANCIAL_CLASS_CD",
                "BBD_PROCEDURE_CD",
                "INFO_GIVEN_BY",
                "SAFEKEEPING_CD",
                "TRAUMA_CD",
                "TRIAGE_CD",
                "TRIAGE_DT_TM",
                "VISITOR_STATUS_CD",
                "VALUABLES_CD",
                "SECURITY_ACCESS_CD",
                "REFER_FACILITY_CD",
                "TRAUMA_DT_TM",
                "ACCOMP_BY_CD",
                "ACCOMMODATION_REASON_CD",
                "CHART_COMPLETE_DT_TM",
                "ZERO_BALANCE_DT_TM",
                "ARCHIVE_DT_TM_EST",
                "ARCHIVE_DT_TM_ACT",
                "PURGE_DT_TM_EST",
                "PURGE_DT_TM_ACT",
                "ENCNTR_COMPLETE_DT_TM",
                "PA_CURRENT_STATUS_CD",
                "PA_CURRENT_STATUS_DT_TM",
                "PARENT_RET_CRITERIA_ID",
                "SERVICE_CATEGORY_CD",
                "CONTRACT_STATUS_CD",
                "EST_LENGTH_OF_STAY",
                "ASSIGN_TO_LOC_DT_TM",
                "ALT_LVL_CARE_CD",
                "PROGRAM_SERVICE_CD",
                "SPECIALTY_UNIT_CD",
                "MENTAL_HEALTH_DT_TM",
                "MENTAL_HEALTH_CD",
                "DOC_RCVD_DT_TM",
                "REFERRAL_RCVD_DT_TM",
                "ALT_LVL_CARE_DT_TM",
                "ALC_DECOMP_DT_TM",
                "REGION_CD",
                "SITTER_REQUIRED_CD",
                "ALC_REASON_CD",
                "PLACEMENT_AUTH_PRSNL_ID",
                "PATIENT_CLASSIFICATION_CD",
                "MENTAL_CATEGORY_CD",
                "PSYCHIATRIC_STATUS_CD",
                "INPATIENT_ADMIT_DT_TM",
                "RESULT_ACCUMULATION_DT_TM",
                "LAST_MENSTRUAL_PERIOD_DT_TM",
                "ONSET_DT_TM",
                "EXPECTED_DELIVERY_DT_TM",
                "LEVEL_OF_SERVICE_CD",
                "ABN_STATUS_CD",
                "PLACE_OF_SVC_ORG_ID",
                "PLACE_OF_SVC_TYPE_CD",
                "PLACE_OF_SVC_ADMIT_DT_TM",
                "PREGNANCY_STATUS_CD",
                "INITIAL_CONTACT_DT_TM",
                "EST_FINANCIAL_RESP_AMT",
                "ACCIDENT_RELATED_IND",
                "REFERRAL_SOURCE_CD",
                "ADMIT_DECISION_DT_TM",
                "LAST_UPDATED",
                "IMPORT_ID",
                "HASH",
                "med_service",
                "service_category",
                "encntr_type",
                "encntr_type_class",
                "encntr_status",
                "admit_type",
                "admit_src",
                "admit_mode",
                "disch_disposition",
                "readmit",
                "accommodation",
                "location",
                "loc_facility",
                "loc_building",
                "loc_nurse_unit",
                "loc_room",
                "refer_facility"
        };
    }

    public CsvCell getEncounterId() {
        return super.getCell("ENCNTR_ID");
    }
    public CsvCell getActiveInd() {
        return super.getCell("ACTIVE_IND");
    }

    @Override
    protected String getFileTypeDescription() {
        return "Cerner Encounter file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}