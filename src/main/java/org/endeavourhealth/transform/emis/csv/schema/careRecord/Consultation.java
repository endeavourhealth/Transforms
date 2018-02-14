package org.endeavourhealth.transform.emis.csv.schema.careRecord;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;

import java.util.UUID;

public class Consultation extends AbstractCsvParser {

    public Consultation(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath, boolean openParser) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, openParser, EmisCsvToFhirTransformer.CSV_FORMAT, EmisCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD, EmisCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {

        if (version.equals(EmisCsvToFhirTransformer.VERSION_5_0)) {
            return new String[]{
                    "ConsultationGuid",
                    "PatientGuid",
                    "OrganisationGuid",
                    "EffectiveDate",
                    "EffectiveDatePrecision",
                    "EnteredDate",
                    //"EnteredTime", //the earliest version (we've seen) doesn't have this column
                    "ClinicianUserInRoleGuid",
                    "EnteredByUserInRoleGuid",
                    "AppointmentSlotGuid",
                    "ConsultationSourceTerm",
                    "ConsultationSourceCodeId",
                    "Complete",
                    "Deleted",
                    "IsConfidential",
                    "ProcessingId"
            };
        } else {
            return new String[]{
                    "ConsultationGuid",
                    "PatientGuid",
                    "OrganisationGuid",
                    "EffectiveDate",
                    "EffectiveDatePrecision",
                    "EnteredDate",
                    "EnteredTime",
                    "ClinicianUserInRoleGuid",
                    "EnteredByUserInRoleGuid",
                    "AppointmentSlotGuid",
                    "ConsultationSourceTerm",
                    "ConsultationSourceCodeId",
                    "Complete",
                    "Deleted",
                    "IsConfidential",
                    "ProcessingId"
            };
        }
    }

    @Override
    protected String getFileTypeDescription() {
        return "Emis consultations file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getConsultationGuid() {
        return super.getCell("ConsultationGuid");
    }
    public CsvCell getPatientGuid() {
        return super.getCell("PatientGuid");
    }
    public CsvCell getOrganisationGuid() {
        return super.getCell("OrganisationGuid");
    }
    public CsvCell getEffectiveDate() {
        return super.getCell("EffectiveDate");
    }
    public CsvCell getEffectiveDatePrecision() {
        return super.getCell("EffectiveDatePrecision");
    }
    public CsvCell getEnteredDate() {
        return super.getCell("EnteredDate");
    }
    public CsvCell getEnteredTime() {
        return super.getCell("EnteredTime");
    }
    public CsvCell getClinicianUserInRoleGuid() {
        return super.getCell("ClinicianUserInRoleGuid");
    }
    public CsvCell getEnteredByUserInRoleGuid() {
        return super.getCell("EnteredByUserInRoleGuid");
    }
    public CsvCell getAppointmentSlotGuid() {
        return super.getCell("AppointmentSlotGuid");
    }
    public CsvCell getConsultationSourceTerm() {
        return super.getCell("ConsultationSourceTerm");
    }
    public CsvCell getComplete() {
        return super.getCell("Complete");
    }
    public CsvCell getDeleted() {
        return super.getCell("Deleted");
    }
    public CsvCell getConsultationSourceCodeId() {
        return super.getCell("ConsultationSourceCodeId");
    }
    public CsvCell getProcessingId() {
        return super.getCell("ProcessingId");
    }
    public CsvCell getIsConfidential() {
        return super.getCell("IsConfidential");
    }

    /*public String getConsultationGuid() {
        return super.getString("ConsultationGuid");
    }
    public String getPatientGuid() {
        return super.getString("PatientGuid");
    }
    public String getOrganisationGuid() {
        return super.getString("OrganisationGuid");
    }
    public Date getEffectiveDate() throws TransformException {
        return super.getDate("EffectiveDate");
    }
    public String getEffectiveDatePrecision() {
        return super.getString("EffectiveDatePrecision");
    }
    public Date getEnteredDate() throws TransformException {
        return super.getDate("EnteredDate");
    }
    public Date getEnteredDateTime() throws TransformException {
        return super.getDateTime("EnteredDate", "EnteredTime");
    }
    public String getClinicianUserInRoleGuid() {
        return super.getString("ClinicianUserInRoleGuid");
    }
    public String getEnteredByUserInRoleGuid() {
        return super.getString("EnteredByUserInRoleGuid");
    }
    public String getAppointmentSlotGuid() {
        return super.getString("AppointmentSlotGuid");
    }
    public String getConsultationSourceTerm() {
        return super.getString("ConsultationSourceTerm");
    }
    public boolean getComplete() {
        return super.getBoolean("Complete");
    }
    public boolean getDeleted() {
        return super.getBoolean("Deleted");
    }
    public Long getConsultationSourceCodeId() {
        return super.getLong("ConsultationSourceCodeId");
    }
    public Integer getProcessingId() {
        return super.getInt("ProcessingId");
    }
    public boolean getIsConfidential() {
        return super.getBoolean("IsConfidential");
    }*/

}
