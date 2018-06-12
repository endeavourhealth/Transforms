package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class OPATT extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(OPATT.class);

    public OPATT(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
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
                "SCHEDULE_ID",
                "ENCNTR_ID",
                "SCH_EVENT_ID",
                "ATTENDANCE_IDENT",
                "APPT_DT_TM",
                "FIRST_ATT_NHS_CD_ALIAS",
                "LAST_DNA_CNCL_DT_TM",
                "MEDICAL_STAFF_TYPE_NHS_CD_ALIAS",
                "OPERATION_STATUS_NHS_CD_ALIAS",
                "OUTCOME_OF_ATT_NHS_CD_ALIAS",
                "LOCATION_CLASS_NHS_CD_ALIAS",
                "TREATMENT_SITE_NHS_ORG_ALIAS",
                "PAT_REFUSED_DATE_IND",
                "FIRST_OFFERED_DT",
                "SECOND_OFFERED_DT",
                "APPT_LOCATION_CD",
                "REASON_FOR_VISIT_TXT",
                "APPT_TYPE_CD",
                "PERSON_ID",
                "FIN_NBR_ID",
                "ATTENDED_DNA_CD",
                "ATTENDED_DNA_NHS_CD_ALIAS",
                "EXPECTED_DUR_OF_APPT_NBR",
                "ACTIVITY_LOC_TYPE_NHS_CD_ALIAS",
                "ENCNTR_CREATE_PRSNL_ID",
                "ENCNTR_UPDT_PRSNL_ID"
        };
    }

    public CsvCell getCDSBatchContentEventId() {
        return super.getCell("#CDS_BATCH_CONTENT_ID");
    }

    public CsvCell getExtractDateTime() {
        return super.getCell("EXTRACT_DT_TM");
    }

    public CsvCell getActiveIndicator() {
        return super.getCell("ACTIVE_IND");
    }

    public CsvCell getEncounterScheduleID() {
        return super.getCell("SCHEDULE_ID");
    }

    public CsvCell getEncounterId() {
        return super.getCell("ENCNTR_ID");
    }

    public CsvCell getScheduleEventId() {
        return super.getCell("SCH_EVENT_ID");
    }

    public CsvCell getAttendanceIdentifier() {
        return super.getCell("ATTENDANCE_IDENT");
    }

    public CsvCell getAppointmentDateTime() {
        return super.getCell("APPT_DT_TM");
    }

    public CsvCell getFirstAttendanceCode() {
        return super.getCell("FIRST_ATT_NHS_CD_ALIAS");
    }

    public CsvCell getLastDNAOrCancelCode() {
        return super.getCell("LAST_DNA_CNCL_DT_TM");
    }

    public CsvCell getMedicalStaffTypeCode() {
        return super.getCell("MEDICAL_STAFF_TYPE_NHS_CD_ALIAS");
    }

    public CsvCell getOperationStatusCode() {
        return super.getCell("OPERATION_STATUS_NHS_CD_ALIAS");
    }

    public CsvCell getAttendanceOutcomeCode() {
        return super.getCell("OUTCOME_OF_ATT_NHS_CD_ALIAS");
    }

    public CsvCell getLocationClass() {
        return super.getCell("LOCATION_CLASS_NHS_CD_ALIAS");
    }

    public CsvCell getTreatmentSiteOrgCode() {
        return super.getCell("TREATMENT_SITE_NHS_ORG_ALIAS");
    }

    public CsvCell getPatientRefusedOfferedDates() {
        return super.getCell("PAT_REFUSED_DATE_IND");
    }

    public CsvCell getFirstDateOffered() {
        return super.getCell("FIRST_OFFERED_DT");
    }

    public CsvCell getSecondDateOffered() {
        return super.getCell("SECOND_OFFERED_DT");
    }

    public CsvCell getLocationCode() {
        return super.getCell("APPT_LOCATION_CD");
    }

    public CsvCell getReasonForVisitText() {
        return super.getCell("REASON_FOR_VISIT_TXT");
    }

    public CsvCell getAppointmentTypeCode() {
        return super.getCell("APPT_TYPE_CD");
    }

    public CsvCell getPatientId() {
        return super.getCell("PERSON_ID");
    }

    public CsvCell getFINNo() {
        return super.getCell("FIN_NBR_ID");
    }

    public CsvCell getAttendendOrDNAMilleniumCode() {
        return super.getCell("ATTENDED_DNA_CD");
    }

    public CsvCell getAttendendOrDNANHSCode() {
        return super.getCell("ATTENDED_DNA_NHS_CD_ALIAS");
    }

    public CsvCell getExpectedAppointmentDuration() {
        return super.getCell("EXPECTED_DUR_OF_APPT_NBR");
    }

    public CsvCell ActivityLocationType() {
        return super.getCell("ACTIVITY_LOC_TYPE_NHS_CD_ALIAS");
    }

    public CsvCell getEncounterCreatedByPersonnelId() {
        return super.getCell("ENCNTR_CREATE_PRSNL_ID");
    }

    public CsvCell getEncounterUpdatedByPersonnelId() {
        return super.getCell("ENCNTR_UPDT_PRSNL_ID");
    }

    @Override
    protected String getFileTypeDescription() {
        return "Cerner Outpatient Attendance file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}