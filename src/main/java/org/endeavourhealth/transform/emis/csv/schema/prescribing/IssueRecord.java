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
        CsvCell ret = super.getCell("ClinicianUserInRoleGuid");
        //special handling for handling mis-spelt column in EMIS test pack
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


}
