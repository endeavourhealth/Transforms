package org.endeavourhealth.transform.emis.csv.schema.careRecord;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;

import java.util.UUID;

public class Observation extends AbstractCsvParser {

    public Observation(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, EmisCsvToFhirTransformer.CSV_FORMAT, EmisCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD, EmisCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[]{
                "ObservationGuid",
                "PatientGuid",
                "OrganisationGuid",
                "EffectiveDate",
                "EffectiveDatePrecision",
                "EnteredDate",
                "EnteredTime",
                "ClinicianUserInRoleGuid",
                "EnteredByUserInRoleGuid",
                "ParentObservationGuid",
                "CodeId",
                "ProblemGuid",
                "AssociatedText",
                "ConsultationGuid",
                "Value",
                "NumericUnit",
                "ObservationType",
                "NumericRangeLow",
                "NumericRangeHigh",
                "DocumentGuid",
                "Deleted",
                "IsConfidential",
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
    public CsvCell getParentObservationGuid() {
        return super.getCell("ParentObservationGuid");
    }
    public CsvCell getCodeId() {
        return super.getCell("CodeId");
    }
    public CsvCell getProblemGuid() {
        return super.getCell("ProblemGuid");
    }
    public CsvCell getAssociatedText() {
        return super.getCell("AssociatedText");
    }
    public CsvCell getConsultationGuid() {
        return super.getCell("ConsultationGuid");
    }
    public CsvCell getValue() {
        return super.getCell("Value");
    }
    public CsvCell getNumericUnit() {
        return super.getCell("NumericUnit");
    }
    public CsvCell getObservationType() {
        return super.getCell("ObservationType");
    }
    public CsvCell getNumericRangeLow() {
        return super.getCell("NumericRangeLow");
    }
    public CsvCell getNumericRangeHigh() {
        return super.getCell("NumericRangeHigh");
    }
    public CsvCell getDocumentGuid() {
        return super.getCell("DocumentGuid");
    }
    public CsvCell getDeleted() {
        return super.getCell("Deleted");
    }
    public CsvCell getIsConfidential() {
        return super.getCell("IsConfidential");
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
    public String getParentObservationGuid() {
        return super.getString("ParentObservationGuid");
    }
    public Long getCodeId() {
        return super.getLong("CodeId");
    }
    public String getProblemGuid() {
        return super.getString("ProblemGuid");
    }
    public String getAssociatedText() {
        return super.getString("AssociatedText");
    }
    public String getConsultationGuid() {
        return super.getString("ConsultationGuid");
    }
    public Double getValue() {
        return super.getDouble("Value");
    }
    public String getNumericUnit() {
        return super.getString("NumericUnit");
    }
    public String getObservationType() {
        return super.getString("ObservationType");
    }
    public Double getNumericRangeLow() {
        return super.getDouble("NumericRangeLow");
    }
    public Double getNumericRangeHigh() {
        return super.getDouble("NumericRangeHigh");
    }
    public String getDocumentGuid() {
        return super.getString("DocumentGuid");
    }
    public boolean getDeleted() {
        return super.getBoolean("Deleted");
    }
    public boolean getIsConfidential() {
        return super.getBoolean("IsConfidential");
    }
    public Integer getProcessingId() {
        return super.getInt("ProcessingId");
    }*/
}
