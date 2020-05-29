package org.endeavourhealth.transform.bhrut.schema;

import org.endeavourhealth.transform.bhrut.BhrutCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class Outpatients extends AbstractCsvParser {

    private static final Logger LOG = LoggerFactory.getLogger(Outpatients.class);

    public Outpatients(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath,
                BhrutCsvToFhirTransformer.CSV_FORMAT,
                BhrutCsvToFhirTransformer.DATE_FORMAT,
                BhrutCsvToFhirTransformer.TIME_FORMAT);
    }


    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "EXTERNAL_ID",
                "PAS_ID",
                "PATHWAY_ID",
                "PURCHASER",
                "ADMIN_CATEGORY_CODE",
                "ADMIN_CATEGORY",
                "BOOKING_TYPE",
                "APPOINTMENT_URGENCY",
                "APPOINTMENT_PRIORITY",
                "FIRST_ATTEND_FLAG",
                "APPT_TYPE_CODE",
                "APPT_TYPE",
                "ATTENDANCE_OUTCOME_DESC",
                "APPOINTMENT_OUTCOME_CODE",
                "APPOINTMENT_OUTCOME",
                "APPOINTMENT_STATUS_CODE",
                "APPOINTMENT_STATUS",
                "APPOINTMENT_DTTM",
                "APPT_ARRIVAL_DTTM",
                "APPT_CALL_DTTM",
                "APPT_SEEN_DTTM",
                "APPT_DEPARTURE_DTTM",
                "BOOKED_DTTM",
                "CANCEL_DTTM",
                "CANCELLED_BY",
                "CANCEL_REASON",
                "HOSPITAL_NAME",
                "HOSPITAL_CODE",
                "LOCATION",
                "CLINICAL_SERVICE",
                "CLINIC_CODE",
                "CLINIC_NAME",
                "CONSULTANT_CODE",
                "CONSULTANT",
                "CONSULTANT_MAIN_SPECIALTY",
                "CONSULTANT_MAIN_SPECIALTY_CODE",
                "SPECIALTY_CODE",
                "SPECIALTY",
                "REFERRAL_EXTERNAL_ID",
                "PRIMARY_PROCEDURE_CODE",
                "SECONDARY_PROCEDURE_CODE_1",
                "SECONDARY_PROCEDURE_CODE_2",
                "SECONDARY_PROCEDURE_CODE_3",
                "SECONDARY_PROCEDURE_CODE_4",
                "SECONDARY_PROCEDURE_CODE_5",
                "SECONDARY_PROCEDURE_CODE_6",
                "SECONDARY_PROCEDURE_CODE_7",
                "SECONDARY_PROCEDURE_CODE_8",
                "SECONDARY_PROCEDURE_CODE_9",
                "SECONDARY_PROCEDURE_CODE_10",
                "SECONDARY_PROCEDURE_CODE_11",
                "PRIMARY_DIAGNOSIS_CODE",
                "SECONDARY_DIAGNOSIS_CODE_1",
                "SECONDARY_DIAGNOSIS_CODE_2",
                "SECONDARY_DIAGNOSIS_CODE_3",
                "DataUpdateStatus"
        };

    }

    public CsvCell getId() {
        return super.getCell("EXTERNAL_ID");
    }

    public CsvCell getPasId() {
        return super.getCell("PAS_ID");
    }

    public CsvCell getPathwayId() {
        return super.getCell("PATHWAY_ID");
    }

    public CsvCell getPurchaser() {
        return super.getCell("PURCHASER");
    }

    public CsvCell getAdminCategoryCode() {
        return super.getCell("ADMIN_CATEGORY_CODE");
    }

    public CsvCell getAdminCategory() {
        return super.getCell("ADMIN_CATEGORY");
    }

    public CsvCell getBookingType() {
        return super.getCell("BOOKING_TYPE");
    }

    public CsvCell getAppointmentUrgency() {
        return super.getCell("APPOINTMENT_URGENCY");
    }

    public CsvCell getAppointmentPriority() {
        return super.getCell("APPOINTMENT_PRIORITY");
    }

    public CsvCell getFirstAttendFlag() {
        return super.getCell("FIRST_ATTEND_FLAG");
    }

    public CsvCell getApptTypeCode() {
        return super.getCell("APPT_TYPE_CODE");
    }

    public CsvCell getApptType() {
        return super.getCell("APPT_TYPE");
    }

    public CsvCell getAttendanceOutcomeDesc() {
        return super.getCell("ATTENDANCE_OUTCOME_DESC");
    }

    public CsvCell getAppointmentOutcomeCode() {
        return super.getCell("APPOINTMENT_OUTCOME_CODE");
    }

    public CsvCell getAppointmentOutcome() {
        return super.getCell("APPOINTMENT_OUTCOME");
    }

    public CsvCell getAppointmentStatusCode() {
        return super.getCell("APPOINTMENT_STATUS_CODE");
    }

    public CsvCell getAppointmentStatus() {
        return super.getCell("APPOINTMENT_STATUS");
    }

    public CsvCell getAppointmentDttm() {
        return super.getCell("APPOINTMENT_DTTM");
    }

    public CsvCell getApptArrivalDttm() {
        return super.getCell("APPT_ARRIVAL_DTTM");
    }

    public CsvCell getApptCallDttm() {
        return super.getCell("APPT_CALL_DTTM");
    }

    public CsvCell getApptSeenDttm() {
        return super.getCell("APPT_SEEN_DTTM");
    }

    public CsvCell getApptDepartureDttm() {
        return super.getCell("APPT_DEPARTURE_DTTM");
    }

    public CsvCell getBookedDttm() {
        return super.getCell("BOOKED_DTTM");
    }

    public CsvCell getCancelDttm() {
        return super.getCell("CANCEL_DTTM");
    }

    public CsvCell getCancelledBy() {
        return super.getCell("CANCELLED_BY");
    }

    public CsvCell getCancelReason() {
        return super.getCell("CANCEL_REASON");
    }

    public CsvCell getHospitalName() {
        return super.getCell("HOSPITAL_NAME");
    }

    public CsvCell getHospitalCode() {
        return super.getCell("HOSPITAL_CODE");
    }

    public CsvCell getLocation() {
        return super.getCell("LOCATION");
    }

    public CsvCell getClinicalService() {
        return super.getCell("CLINICAL_SERVICE");
    }

    public CsvCell getClinicCode() {
        return super.getCell("CLINIC_CODE");
    }

    public CsvCell getClinicName() {
        return super.getCell("CLINIC_NAME");
    }

    public CsvCell getConsultantCode() {
        return super.getCell("CONSULTANT_CODE");
    }

    public CsvCell getConsultant() {
        return super.getCell("CONSULTANT");
    }

    public CsvCell getConsultantMainSpecialty() {
        return super.getCell("CONSULTANT_MAIN_SPECIALTY");
    }

    public CsvCell getConsultantMainSpecialtyCode() {
        return super.getCell("CONSULTANT_MAIN_SPECIALTY_CODE");
    }

    public CsvCell getSpecialtyCode() {
        return super.getCell("SPECIALTY_CODE");
    }

    public CsvCell getSpecialty() {
        return super.getCell("SPECIALTY");
    }

    public CsvCell getReferralExternalId() {
        return super.getCell("REFERRAL_EXTERNAL_ID");
    }

    public CsvCell getPrimaryProcedureCode() {
        return super.getCell("PRIMARY_PROCEDURE_CODE");
    }

    public CsvCell getSecondaryProcedureCode1() {
        return super.getCell("SECONDARY_PROCEDURE_CODE_1");
    }

    public CsvCell getSecondaryProcedureCode2() {
        return super.getCell("SECONDARY_PROCEDURE_CODE_2");
    }

    public CsvCell getSecondaryProcedureCode3() {
        return super.getCell("SECONDARY_PROCEDURE_CODE_3");
    }

    public CsvCell getSecondaryProcedureCode4() {
        return super.getCell("SECONDARY_PROCEDURE_CODE_4");
    }

    public CsvCell getSecondaryProcedureCode5() {
        return super.getCell("SECONDARY_PROCEDURE_CODE_5");
    }

    public CsvCell getSecondaryProcedureCode6() {
        return super.getCell("SECONDARY_PROCEDURE_CODE_6");
    }

    public CsvCell getSecondaryProcedureCode7() {
        return super.getCell("SECONDARY_PROCEDURE_CODE_7");
    }

    public CsvCell getSecondaryProcedureCode8() {
        return super.getCell("SECONDARY_PROCEDURE_CODE_8");
    }

    public CsvCell getSecondaryProcedureCode9() {
        return super.getCell("SECONDARY_PROCEDURE_CODE_9");
    }

    public CsvCell getSecondaryProcedureCode10() {
        return super.getCell("SECONDARY_PROCEDURE_CODE_10");
    }

    public CsvCell getSecondaryProcedureCode11() {
        return super.getCell("SECONDARY_PROCEDURE_CODE_11");
    }

    public CsvCell getPrimaryDiagnosisCode() {
        return super.getCell("PRIMARY_DIAGNOSIS_CODE");
    }

    public CsvCell getSecondaryDiagnosisCode1() {
        return super.getCell("SECONDARY_DIAGNOSIS_CODE_1");
    }

    public CsvCell getSecondaryDiagnosisCode2() {
        return super.getCell("SECONDARY_DIAGNOSIS_CODE_2");
    }

    public CsvCell getSecondaryDiagnosisCode3() {
        return super.getCell("SECONDARY_DIAGNOSIS_CODE_3");
    }

    public CsvCell getDataUpdateStatus() {
        return super.getCell("DataUpdateStatus");
    }


    protected String getFileTypeDescription() {
        return "Bhrut Outpatients Entry file ";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }
}
