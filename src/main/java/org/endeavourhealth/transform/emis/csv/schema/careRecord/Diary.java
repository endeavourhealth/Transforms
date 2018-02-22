package org.endeavourhealth.transform.emis.csv.schema.careRecord;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;

import java.util.UUID;

public class Diary extends AbstractCsvParser {

    public Diary(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, EmisCsvToFhirTransformer.CSV_FORMAT, EmisCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD, EmisCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {

        //the EMIS test pack has mis-spellings of column names, so having to handle this here
        if (version.equals(EmisCsvToFhirTransformer.VERSION_5_0)
                || version.equals(EmisCsvToFhirTransformer.VERSION_5_1)) {
            return new String[]{
                    "DiaryGuid",
                    "PatientGuid",
                    "OrganisationGuid",
                    "EffectiveDate",
                    "EffectiveDatePrecision",
                    "EnteredDate",
                    "EnteredTime",
                    "ClinicanUserInRoleGuid",
                    "EnteredByUserInRoleGuid",
                    "CodeId",
                    "OriginalTerm",
                    "AssociatedText",
                    "DurationTerm",
                    "LocationTypeDescription",
                    "Deleted",
                    "IsConfidential",
                    "IsActive",
                    "IsComplete",
                    "ConsultationGuid",
                    "ProcessingId"};
        } else {
            return new String[]{
                    "DiaryGuid",
                    "PatientGuid",
                    "OrganisationGuid",
                    "EffectiveDate",
                    "EffectiveDatePrecision",
                    "EnteredDate",
                    "EnteredTime",
                    "ClinicianUserInRoleGuid",
                    "EnteredByUserInRoleGuid",
                    "CodeId",
                    "OriginalTerm",
                    "AssociatedText",
                    "DurationTerm",
                    "LocationTypeDescription",
                    "Deleted",
                    "IsConfidential",
                    "IsActive",
                    "IsComplete",
                    "ConsultationGuid",
                    "ProcessingId"};
        }

    }

    @Override
    protected String getFileTypeDescription() {
        return "Emis diary file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getDiaryGuid() {
        return super.getCell("DiaryGuid");
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
    public CsvCell getCodeId() {
        return super.getCell("CodeId");
    }
    public CsvCell getOriginalTerm() {
        return super.getCell("OriginalTerm");
    }
    public CsvCell getAssociatedText() {
        return super.getCell("AssociatedText");
    }
    public CsvCell getDurationTerm() {
        return super.getCell("DurationTerm");
    }
    public CsvCell getLocationTypeDescription() {
        return super.getCell("LocationTypeDescription");
    }
    public CsvCell getConsultationGuid() {
        return super.getCell("ConsultationGuid");
    }
    public CsvCell getIsConfidential() {
        return super.getCell("IsConfidential");
    }
    public CsvCell getIsActive() {
        return super.getCell("IsActive");
    }
    public CsvCell getIsComplete() {
        return super.getCell("IsComplete");
    }
    public CsvCell getDeleted() {
        return super.getCell("Deleted");
    }
    public CsvCell getProcessingId() {
        return super.getCell("ProcessingId");
    }

    /**
     * special function for mis-spelt column name in EMIS test pack
     */
    public CsvCell getClinicanUserInRoleGuid() {
        return super.getCell("ClinicanUserInRoleGuid");
    }



    /*public String getDiaryGuid() {
        return super.getString("DiaryGuid");
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
    public Date getEnteredDateTime() throws TransformException {
        return super.getDateTime("EnteredDate", "EnteredTime");
    }
    public String getClinicianUserInRoleGuid() {
        return super.getString("ClinicianUserInRoleGuid");
    }
    public String getEnteredByUserInRoleGuid() {
        return super.getString("EnteredByUserInRoleGuid");
    }
    public Long getCodeId() {
        return super.getLong("CodeId");
    }
    public String getOriginalTerm() {
        return super.getString("OriginalTerm");
    }
    public String getAssociatedText() {
        return super.getString("AssociatedText");
    }
    public String getDurationTerm() {
        return super.getString("DurationTerm");
    }
    public String getLocationTypeDescription() {
        return super.getString("LocationTypeDescription");
    }
    public String getConsultationGuid() {
        return super.getString("ConsultationGuid");
    }
    public boolean getIsConfidential() {
        return super.getBoolean("IsConfidential");
    }
    public boolean getIsActive() {
        return super.getBoolean("IsActive");
    }
    public boolean getIsComplete() {
        return super.getBoolean("IsComplete");
    }
    public boolean getDeleted() {
        return super.getBoolean("Deleted");
    }
    public Integer getProcessingId() {
        return super.getInt("ProcessingId");
    }

    *//**
     * special function for mis-spelt column name in EMIS test pack
     *//*
    public String getClinicanUserInRoleGuid() {
        return super.getString("ClinicanUserInRoleGuid");
    }*/

}
