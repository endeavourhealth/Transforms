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
    public CsvCell getReferralTargetOrganisationGuid() {
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
    public CsvCell getReferralEpisodeDischargeLetterIssuedDate() {
        return super.getCell("ReferralEpisodeDischargeLetterIssuedDate");
    }
    public CsvCell getReferralClosureReasonCodeId() {
        return super.getCell("ReferralClosureReasonCodeId");
    }
    public CsvCell getProcessingId() {
        return super.getCell("ProcessingId");
    }

}
