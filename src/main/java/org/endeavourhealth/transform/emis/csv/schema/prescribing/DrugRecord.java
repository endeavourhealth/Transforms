package org.endeavourhealth.transform.emis.csv.schema.prescribing;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;

import java.util.UUID;

public class DrugRecord extends AbstractCsvParser {

    public DrugRecord(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath, boolean openParser) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, openParser, EmisCsvToFhirTransformer.CSV_FORMAT, EmisCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD, EmisCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {

        if (version.equals(EmisCsvToFhirTransformer.VERSION_5_0)) {
            return new String[]{
                    "DrugRecordGuid",
                    "PatientGuid",
                    "OrganisationGuid",
                    "EffectiveDate",
                    "EffectiveDatePrecision",
                    "EnteredDate",
                    //"EnteredTime", //not present in this older version
                    "ClinicanUserInRoleGuid", //mis-spelled column name in this version
                    "EnteredByUserInRoleGuid",
                    "CodeId",
                    "Dosage",
                    "Quantity",
                    "QuantityUnit",
                    "ProblemObservationGuid",
                    "PrescriptionType",
                    "IsActive",
                    "CancellationDate",
                    "NumberOfIssues",
                    "NumberOfIssuesAuthorised",
                    "IsConfidential",
                    "Deleted",
                    "ProcessingId"
            };
        } else if (version.equals(EmisCsvToFhirTransformer.VERSION_5_1)) {
            return new String[]{
                    "DrugRecordGuid",
                    "PatientGuid",
                    "OrganisationGuid",
                    "EffectiveDate",
                    "EffectiveDatePrecision",
                    "EnteredDate",
                    "EnteredTime",
                    "ClinicanUserInRoleGuid", //mis-spelled column name in this version
                    "EnteredByUserInRoleGuid",
                    "CodeId",
                    "Dosage",
                    "Quantity",
                    "QuantityUnit",
                    "ProblemObservationGuid",
                    "PrescriptionType",
                    "IsActive",
                    "CancellationDate",
                    "NumberOfIssues",
                    "NumberOfIssuesAuthorised",
                    "IsConfidential",
                    "Deleted",
                    "ProcessingId"
            };
        } else {
            return new String[]{
                    "DrugRecordGuid",
                    "PatientGuid",
                    "OrganisationGuid",
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
                    "PrescriptionType",
                    "IsActive",
                    "CancellationDate",
                    "NumberOfIssues",
                    "NumberOfIssuesAuthorised",
                    "IsConfidential",
                    "Deleted",
                    "ProcessingId"
            };
        }
    }

    @Override
    protected String getFileTypeDescription() {
        return "Emis drug record file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getDrugRecordGuid() {
        return super.getCell("DrugRecordGuid");
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
    public CsvCell getDosage() {
        return super.getCell("Dosage");
    }
    public CsvCell getQuantity() {
        return super.getCell("Quantity");
    }
    public CsvCell getQuantityUnit() {
        return super.getCell("QuantityUnit");
    }
    public CsvCell getProblemObservationGuid() {
        return super.getCell("ProblemObservationGuid");
    }
    public CsvCell getPrescriptionType() {
        return super.getCell("PrescriptionType");
    }
    public CsvCell getIsActive() {
        return super.getCell("IsActive");
    }
    public CsvCell getCancellationDate() {
        return super.getCell("CancellationDate");
    }
    public CsvCell getNumberOfIssues() {
        return super.getCell("NumberOfIssues");
    }
    public CsvCell getNumberOfIssuesAuthorised() {
        return super.getCell("NumberOfIssuesAuthorised");
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
     * special function to handle mis-spelt column name in EMIS test pack
     */
    public CsvCell getClinicanUserInRoleGuid() {
        return super.getCell("ClinicanUserInRoleGuid");
    }

    /*public String getDrugRecordGuid() {
        return super.getString("DrugRecordGuid");
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
    public String getDosage() {
        return super.getString("Dosage");
    }
    public Double getQuantity() {
        return super.getDouble("Quantity");
    }
    public String getQuantityUnit() {
        return super.getString("QuantityUnit");
    }
    public String getProblemObservationGuid() {
        return super.getString("ProblemObservationGuid");
    }
    public String getPrescriptionType() {
        return super.getString("PrescriptionType");
    }
    public boolean getIsActive() {
        return super.getBoolean("IsActive");
    }
    public Date getCancellationDate() throws TransformException {
        return super.getDate("CancellationDate");
    }
    public Integer getNumberOfIssues() {
        return super.getInt("NumberOfIssues");
    }
    public Integer getNumberOfIssuesAuthorised() {
        return super.getInt("NumberOfIssuesAuthorised");
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
     * special function to handle mis-spelt column name in EMIS test pack
     *//*
    public String getClinicanUserInRoleGuid() {
        return super.getString("ClinicanUserInRoleGuid");
    }*/
}
