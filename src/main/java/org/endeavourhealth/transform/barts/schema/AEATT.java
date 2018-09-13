package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class AEATT extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(AEATT.class);

    //public static final String DATE_FORMAT = "dd/mm/yyyy";
    //public static final String TIME_FORMAT = "hh:mm:ss";
    //public static final String DATE_TIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT;

    public AEATT(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BartsCsvToFhirTransformer.CSV_FORMAT,
                BartsCsvToFhirTransformer.DATE_FORMAT,
                BartsCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "#CDS_BATCH_CONTENT_ID",
                "EXTRACT_DT_TM",
                "ACTIVE_IND",
                "ENCNTR_ID",
                "ATTENDANCE_NBR_IDENT",
                "PERSON_ID",
                "ADMISSION_DT_TM",
                "ARRIVAL_DT_TM",
                "ARRIVAL_MODE_CD",
                "ATTENDANCE_CATEGORY_CD",
                "PRESENTING_COMP_TXT",
                "RECEPTIONIST_PRSNL_ID",
                "SOURCE_OF_REFERRAL_CD",
                "STAFF_LOCAL_IDENT",
                "AMBULANCE_IDENT",
                "INCIDENT_DT_TM",
                "INCIDENT_LOC_CD",
                "ROAD_TRAFFIC_ACC_ADDR_TXT",
                "ROAD_TRAFFIC_ACC_POLICE_IDENT",
                "CHECKIN_DT_TM",
                "CHECKOUT_DT_TM",
                "PATIENT_GRP_NHS_CD_ALIAS",
                "TRACKING_GRP_CD",
                "TRIAGE_START_DT_TM",
                "TRIAGE_COMPLETE_DT_TM",
                "TRIAGE_PRSNL_ID",
                "HCP_FIRST_ASSIGNED_PRSNL_ID",
                "FIRST_ASSESSMENT_DT_TM",
                "FIRST_SPEC_REQ_DT_TM",
                "FIRST_SPEC_ARR_DT_TM",
                "FIRST_SPECIALIST_PRSNL_ID",
                "FIRST_XRAY_IN_DT_TM",
                "FIRST_XRAY_OUT_DT_TM",
                "SEEN_FOR_TREAT_DT_TM",
                "DECISION_TO_ADMIT_DT_TM",
                "CONCLUSION_DT_TM",
                "ATTENDANCE_DISPOSAL_CD",
                "DEPARTURE_DT_TM",
                "DEPARTMENT_TYPE_NHS_CD_ALIAS",
                "LAST_LOC_CD",
                "SCHOOL_NURSERY_ORG_ID",
                "RESP_HCP_PRSNL_ID",
                "STREAM_CD",
                "SITE_TREAT_NHS_ORG_ALIAS",
                "REFERRER_PRSNL_ID",
                "DISCHARGE_DISPOSITION_CD",
                "TRIAGE_CATEGORY_NBR",
                "AMBULANCE_INCIDENT_IDENT",
                "ENCNTR_UPDT_PRSNL_ID",
                "ENCNTR_CREATE_PRSNL_ID"
        };
    }


    public CsvCell getCdsBatchContentId() {
        return super.getCell("#CDS_BATCH_CONTENT_ID");
    }

    public CsvCell getExtractDateTime() {
        return super.getCell("EXTRACT_DT_TM");
    }

    public CsvCell getActiveIndicator() {
        return super.getCell("ACTIVE_IND");
    }

    public CsvCell getEncounterId() {
        return super.getCell("ENCNTR_ID");
    }

    public CsvCell getAttendanceNbrIdent() {
        return super.getCell("ATTENDANCE_NBR_IDENT");
    }

    public CsvCell getPersonId() {
        return super.getCell("PERSON_ID");
    }

    public CsvCell getAdmissionDateTime() {
        return super.getCell("ADMISSION_DT_TM");
    }

    public CsvCell getArrivalDateTime() {
        return super.getCell("ARRIVAL_DT_TM");
    }

    public CsvCell getArrivalModeCode() {
        return super.getCell("ARRIVAL_MODE_CD");
    }

    public CsvCell getAttendanceCategoryCode() {
        return super.getCell("ATTENDANCE_CATEGORY_CD");
    }

    public CsvCell getPresentingCompTxt() {
        return super.getCell("PRESENTING_COMP_TXT");
    }

    public CsvCell getReceptionistPersonId() {
        return super.getCell("RECEPTIONIST_PRSNL_ID");
    }

    public CsvCell getSourceOfReferralCode() {
        return super.getCell("SOURCE_OF_REFERRAL_CD");
    }

    public CsvCell getStaffLocalId() {
        return super.getCell("STAFF_LOCAL_IDENT");
    }

    public CsvCell getAmbulanceId() {
        return super.getCell("AMBULANCE_IDENT");
    }

    public CsvCell getIncidentDateTime() {
        return super.getCell("INCIDENT_DT_TM");
    }

    public CsvCell getIncidentLocCode() {
        return super.getCell("INCIDENT_LOC_CD");
    }

    public CsvCell getRtaAddrTxt() {
        return super.getCell("ROAD_TRAFFIC_ACC_ADDR_TXT");
    }

    public CsvCell getRtaPoliceId() {
        return super.getCell("ROAD_TRAFFIC_ACC_POLICE_IDENT");
    }

    public CsvCell getCheckInDateTime() {
        return super.getCell("CHECKIN_DT_TM");
    }

    public CsvCell getCheckOutDateTime() {
        return super.getCell("CHECKOUT_DT_TM");
    }

    public CsvCell getPatientGrpNhsCdAlias() {
        return super.getCell("PATIENT_GRP_NHS_CD_ALIAS");
    }

    public CsvCell getTrackingGrpCode() {
        return super.getCell("TRACKING_GRP_CD");
    }

    public CsvCell getTriageStartDateTime() {
        return super.getCell("TRIAGE_START_DT_TM");
    }

    public CsvCell getTriageCompleteDateTime() {
        return super.getCell("TRIAGE_COMPLETE_DT_TM");
    }

    public CsvCell getTriagePersonId() {
        return super.getCell("TRIAGE_PRSNL_ID");
    }

    public CsvCell getHcpFirstAssignedPersonId() {
        return super.getCell("HCP_FIRST_ASSIGNED_PRSNL_ID");
    }

    public CsvCell getFirstAssessDateTime() {
        return super.getCell("FIRST_ASSESSMENT_DT_TM");
    }

    public CsvCell getFirstSpecReqDateTime() {
        return super.getCell("FIRST_SPEC_REQ_DT_TM");
    }

    public CsvCell getFirstSpecArriveDateTime() {
        return super.getCell("FIRST_SPEC_ARR_DT_TM");
    }

    public CsvCell getFirstSpecPersonId() {
        return super.getCell("FIRST_SPECIALIST_PRSNL_ID");
    }

    public CsvCell getFirstXrayInDateTime() {
        return super.getCell("FIRST_XRAY_IN_DT_TM");
    }

    public CsvCell getFirstXrayOutDateTime() {
        return super.getCell("FIRST_XRAY_OUT_DT_TM");
    }

    public CsvCell getSeenForTreatDateTime() {
        return super.getCell("SEEN_FOR_TREAT_DT_TM");
    }

    public CsvCell getDecisionToAdmitDateTime() {
        return super.getCell("DECISION_TO_ADMIT_DT_TM");
    }

    public CsvCell getConclusionDateTime() {
        return super.getCell("CONCLUSION_DT_TM");
    }

    public CsvCell getAttendanceDisposalCode() {
        return super.getCell("ATTENDANCE_DISPOSAL_CD");
    }

    public CsvCell getDepartureDateTime() {
        return super.getCell("DEPARTURE_DT_TM");
    }

    public CsvCell getDeptTypeNhsCodeAlias() {
        return super.getCell("DEPARTMENT_TYPE_NHS_CD_ALIAS");
    }

    public CsvCell getLastLocCode() {
        return super.getCell("LAST_LOC_CD");
    }

    public CsvCell getSchoolNurseryOrgId() {
        return super.getCell("SCHOOL_NURSERY_ORG_ID");
    }

    public CsvCell getRespHcpPersonId() {
        return super.getCell("RESP_HCP_PRSNL_ID");
    }

    public CsvCell getStreamCode() {
        return super.getCell("STREAM_CD");
    }

    public CsvCell getSiteTreatNhsOrgAlias() {
        return super.getCell("SITE_TREAT_NHS_ORG_ALIAS");
    }

    public CsvCell getReferralPersonId() {
        return super.getCell("REFERRER_PRSNL_ID");
    }

    public CsvCell getDischargeDispositionCode() {
        return super.getCell("DISCHARGE_DISPOSITION_CD");
    }

    public CsvCell getTriageCatNbr() {
        return super.getCell("TRIAGE_CATEGORY_NBR");
    }

    public CsvCell getAmbulaneIncidentId() {
        return super.getCell("AMBULANCE_INCIDENT_IDENT");
    }

    public CsvCell getEncounterUpdatePersonId() {
        return super.getCell("ENCNTR_UPDT_PRSNL_ID");
    }

    public CsvCell getEncounterCreatePersonId() {
        return super.getCell("ENCNTR_CREATE_PRSNL_ID");
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }


}