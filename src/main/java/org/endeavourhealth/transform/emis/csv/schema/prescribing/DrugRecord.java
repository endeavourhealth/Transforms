package org.endeavourhealth.transform.emis.csv.schema.prescribing;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;

import java.util.UUID;

public class DrugRecord extends AbstractCsvParser {

    public DrugRecord(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, EmisCsvToFhirTransformer.CSV_FORMAT, EmisCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD, EmisCsvToFhirTransformer.TIME_FORMAT);
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
        } else if (version.equals(EmisCsvToFhirTransformer.VERSION_5_5)
                || version.equals(EmisCsvToFhirTransformer.VERSION_8_0_1)) {
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
        } else if (version.equals(EmisCsvToFhirTransformer.VERSION_8_1)
                || version.equals(EmisCsvToFhirTransformer.VERSION_8_2)
                || version.equals(EmisCsvToFhirTransformer.VERSION_8_3))//SD:1190
        { //SD:1090
            //SD-1016 : addition of ExpiryDate column
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
                    "ExpiryDate",
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
        CsvCell ret = super.getCell("ClinicianUserInRoleGuid");
        //special handling for mis-spelled column name in emis test dta
        if (ret == null) {
            ret = super.getCell("ClinicanUserInRoleGuid");
        }
        return ret;
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
}
