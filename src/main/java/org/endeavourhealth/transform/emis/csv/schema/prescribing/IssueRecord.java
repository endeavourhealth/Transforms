package org.endeavourhealth.transform.emis.csv.schema.prescribing;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;

import java.util.UUID;

public class IssueRecord extends AbstractCsvParser {

    public IssueRecord(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, EmisCsvToFhirTransformer.CSV_FORMAT, EmisCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD, EmisCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {

        if (version.equals(EmisCsvToFhirTransformer.VERSION_5_0)) {
            return new String[]{
                    "IssueRecordGuid",
                    "PatientGuid",
                    "OrganisationGuid",
                    "DrugRecordGuid",
                    "EffectiveDate",
                    "EffectiveDatePrecision",
                    "EnteredDate",
                    //"EnteredTime", //not present in this earlier version
                    "ClinicanUserInRoleGuid", //mis-spelled column name
                    "EnteredByUserInRoleGuid",
                    "CodeId",
                    "Dosage",
                    "Quantity",
                    "QuantityUnit",
                    "ProblemObservationGuid",
                    "CourseDurationInDays",
                    "EstimatedNhsCost",
                    "IsConfidential",
                    "Deleted",
                    "ProcessingId"
            };
        } else if (version.equals(EmisCsvToFhirTransformer.VERSION_5_1)) {
            return new String[]{
                    "IssueRecordGuid",
                    "PatientGuid",
                    "OrganisationGuid",
                    "DrugRecordGuid",
                    "EffectiveDate",
                    "EffectiveDatePrecision",
                    "EnteredDate",
                    "EnteredTime",
                    "ClinicanUserInRoleGuid", //mis-spelled column name
                    "EnteredByUserInRoleGuid",
                    "CodeId",
                    "Dosage",
                    "Quantity",
                    "QuantityUnit",
                    "ProblemObservationGuid",
                    "CourseDurationInDays",
                    "EstimatedNhsCost",
                    "IsConfidential",
                    "Deleted",
                    "ProcessingId"
            };
        } else {
            return new String[]{
                    "IssueRecordGuid",
                    "PatientGuid",
                    "OrganisationGuid",
                    "DrugRecordGuid",
                    "EffectiveDate",
                    "EffectiveDatePrecision",
                    "EnteredDate",
                    "EnteredTime",
                    "ClinicianUserInRoleGuid",
                    "EnteredByUserInRoleGuid",
                    "CodeId",
                    "Dosage",
                    "Quantity",
                    "QuantityUnit",
                    "ProblemObservationGuid",
                    "CourseDurationInDays",
                    "EstimatedNhsCost",
                    "IsConfidential",
                    "Deleted",
                    "ProcessingId"
            };
        }
    }


    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getIssueRecordGuid() {
        return super.getCell("IssueRecordGuid");
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
    public CsvCell getQuantity() {
        return super.getCell("Quantity");
    }
    public CsvCell getCourseDurationInDays() {
        return super.getCell("CourseDurationInDays");
    }
    public CsvCell getEstimatedNhsCost() {
        return super.getCell("EstimatedNhsCost");
    }
    public CsvCell getProblemObservationGuid() {
        return super.getCell("ProblemObservationGuid");
    }
    public CsvCell getDosage() {
        return super.getCell("Dosage");
    }
    public CsvCell getQuantityUnit() {
        return super.getCell("QuantityUnit");
    }
    public CsvCell getDrugRecordGuid() {
        return super.getCell("DrugRecordGuid");
    }
    public CsvCell getDeleted() {
        return super.getCell("Deleted");
    }
    public CsvCell getProcessingId() {
        return super.getCell("ProcessingId");
    }
    public CsvCell getIsConfidential() {
        return super.getCell("IsConfidential");
    }

    /**
     * special function for handling mis-spelt column in EMIS test pack
     */
    public CsvCell getClinicanUserInRoleGuid() {
        return super.getCell("ClinicanUserInRoleGuid");
    }

    /*public String getIssueRecordGuid() {
        return super.getString("IssueRecordGuid");
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
    public Long getCodeId() {
        return super.getLong("CodeId");
    }
    public Double getQuantity() {
        return super.getDouble("Quantity");
    }
    public Integer getCourseDurationInDays() {
        return super.getInt("CourseDurationInDays");
    }
    public Double getEstimatedNhsCost() {
        return super.getDouble("EstimatedNhsCost");
    }
    public String getProblemObservationGuid() {
        return super.getString("ProblemObservationGuid");
    }
    public String getDosage() {
        return super.getString("Dosage");
    }
    public String getQuantityUnit() {
        return super.getString("QuantityUnit");
    }
    public String getDrugRecordGuid() {
        return super.getString("DrugRecordGuid");
    }
    public boolean getDeleted() {
        return super.getBoolean("Deleted");
    }
    public Integer getProcessingId() {
        return super.getInt("ProcessingId");
    }
    public boolean getIsConfidential() {
        return super.getBoolean("IsConfidential");
    }

    *//**
     * special function for handling mis-spelt column in EMIS test pack
     *//*
    public String getClinicanUserInRoleGuid() {
        return super.getString("ClinicanUserInRoleGuid");
    }*/
}
