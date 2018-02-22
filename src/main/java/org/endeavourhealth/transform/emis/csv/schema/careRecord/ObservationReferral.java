package org.endeavourhealth.transform.emis.csv.schema.careRecord;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;

import java.util.UUID;

public class ObservationReferral extends AbstractCsvParser {

    public ObservationReferral(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, EmisCsvToFhirTransformer.CSV_FORMAT, EmisCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD, EmisCsvToFhirTransformer.TIME_FORMAT);
    }


    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "ObservationGuid",
                "PatientGuid",
                "OrganisationGuid",
                "ReferralTargetOrganisationGuid",
                "ReferralUrgency",
                "ReferralServiceType",
                "ReferralMode",
                "ReferralReceivedDate",
                "ReferralReceivedTime",
                "ReferralEndDate",
                "ReferralSourceId",
                "ReferralSourceOrganisationGuid",
                "ReferralUBRN",
                "ReferralReasonCodeId",
                "ReferringCareProfessionalStaffGroupCodeId",
                "ReferralEpisodeRTTMeasurementTypeId",
                "ReferralEpisodeClosureDate",
                "ReferralEpisodeDischargeLetterIssuedDate",
                "ReferralClosureReasonCodeId",
                "ProcessingId"
        };
    }

    @Override
    protected String getFileTypeDescription() {
        return "Emis referrals file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getObservationGuid() {
        return super.getCell("ObservationGuid");
    }
    public CsvCell getPatientGuid() {
        return super.getCell("PatientGuid");
    }
    public CsvCell getOrganisationGuid() {
        return super.getCell("OrganisationGuid");
    }
    public CsvCell getReferalTargetOrganisationGuid() {
        return super.getCell("ReferralTargetOrganisationGuid");
    }
    public CsvCell getReferralUrgency() {
        return super.getCell("ReferralUrgency");
    }
    public CsvCell getReferralMode() {
        return super.getCell("ReferralMode");
    }
    public CsvCell getReferralServiceType() {
        return super.getCell("ReferralServiceType");
    }
    public CsvCell getReferralReceivedDate() {
        return super.getCell("ReferralReceivedDate");
    }
    public CsvCell getReferralReceivedTime() {
        return super.getCell("ReferralReceivedTime");
    }
    public CsvCell getReferralEndDate() {
        return super.getCell("ReferralEndDate");
    }
    public CsvCell getReferralSourceId() {
        return super.getCell("ReferralSourceId");
    }
    public CsvCell getReferralSourceOrganisationGuid() {
        return super.getCell("ReferralSourceOrganisationGuid");
    }
    public CsvCell getReferralUBRN() {
        return super.getCell("ReferralUBRN");
    }
    public CsvCell getReferralReasonCodeId() {
        return super.getCell("ReferralReasonCodeId");
    }
    public CsvCell getReferringCareProfessionalStaffGroupCodeId() {
        return super.getCell("ReferringCareProfessionalStaffGroupCodeId");
    }
    public CsvCell getReferralEpisodeRTTMeasurmentTypeId() {
        return super.getCell("ReferralEpisodeRTTMeasurementTypeId");
    }
    public CsvCell getReferralEpisodeClosureDate() {
        return super.getCell("ReferralEpisodeClosureDate");
    }
    public CsvCell getReferralEpisideDischargeLetterIssuedDate() {
        return super.getCell("ReferralEpisodeDischargeLetterIssuedDate");
    }
    public CsvCell getReferralClosureReasonCodeId() {
        return super.getCell("ReferralClosureReasonCodeId");
    }
    public CsvCell getProcessingId() {
        return super.getCell("ProcessingId");
    }
    
    /*public String getObservationGuid() {
        return super.getString("ObservationGuid");
    }
    public String getPatientGuid() {
        return super.getString("PatientGuid");
    }
    public String getOrganisationGuid() {
        return super.getString("OrganisationGuid");
    }
    public String getReferalTargetOrganisationGuid() {
        return super.getString("ReferralTargetOrganisationGuid");
    }
    public String getReferralUrgency() {
        return super.getString("ReferralUrgency");
    }
    public String getReferralMode() {
        return super.getString("ReferralMode");
    }
    public String getReferralServiceType() {
        return super.getString("ReferralServiceType");
    }
    public Date getReferralReceivedDateTime() throws TransformException {
        return super.getDateTime("ReferralReceivedDate", "ReferralReceivedTime");
    }
    public Date getReferralEndDate() throws TransformException {
        return super.getDate("ReferralEndDate");
    }
    public Long getReferralSourceId() {
        return super.getLong("ReferralSourceId");
    }
    public String getReferralSourceOrganisationGuid() {
        return super.getString("ReferralSourceOrganisationGuid");
    }
    public String getReferralUBRN() {
        return super.getString("ReferralUBRN");
    }
    public Long getReferralReasonCodeId() {
        return super.getLong("ReferralReasonCodeId");
    }
    public Long getReferringCareProfessionalStaffGroupCodeId() {
        return super.getLong("ReferringCareProfessionalStaffGroupCodeId");
    }
    public Long getReferralEpisodeRTTMeasurmentTypeId() {
        return super.getLong("ReferralEpisodeRTTMeasurementTypeId");
    }
    public Date getReferralEpisodeClosureDate() throws TransformException {
        return super.getDate("ReferralEpisodeClosureDate");
    }
    public Date getReferralEpisideDischargeLetterIssuedDate() throws TransformException {
        return super.getDate("ReferralEpisodeDischargeLetterIssuedDate");
    }
    public Long getReferralClosureReasonCodeId() {
        return super.getLong("ReferralClosureReasonCodeId");
    }
    public Integer getProcessingId() {
        return super.getInt("ProcessingId");
    }*/
}
