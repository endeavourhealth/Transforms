package org.endeavourhealth.transform.emis.csv.schema.careRecord;

import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.emis.EmisCsvToFhirTransformer;

import java.util.UUID;

public class Problem extends AbstractCsvParser {

    public Problem(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, EmisCsvToFhirTransformer.CSV_FORMAT, EmisCsvToFhirTransformer.DATE_FORMAT_YYYY_MM_DD, EmisCsvToFhirTransformer.TIME_FORMAT);
    }

    @Override
    protected String[] getCsvHeaders(String version) {

        //the older versions didn't have the Deleted column
        if (version.equals(EmisCsvToFhirTransformer.VERSION_5_0)
                || version.equals(EmisCsvToFhirTransformer.VERSION_5_1)) {
            return new String[]{
                    "ObservationGuid",
                    "PatientGuid",
                    "OrganisationGuid",
                    "ParentProblemObservationGuid",
                    "Comment",
                    "EndDate",
                    "EndDatePrecision",
                    "ExpectedDuration",
                    "LastReviewDate",
                    "LastReviewDatePrecision",
                    "LastReviewUserInRoleGuid",
                    "ParentProblemRelationship",
                    "ProblemStatusDescription",
                    "SignificanceDescription",
                    "ProcessingId"
            };
        } else {
            return new String[]{
                    "ObservationGuid",
                    "PatientGuid",
                    "OrganisationGuid",
                    "ParentProblemObservationGuid",
                    "Deleted",
                    "Comment",
                    "EndDate",
                    "EndDatePrecision",
                    "ExpectedDuration",
                    "LastReviewDate",
                    "LastReviewDatePrecision",
                    "LastReviewUserInRoleGuid",
                    "ParentProblemRelationship",
                    "ProblemStatusDescription",
                    "SignificanceDescription",
                    "ProcessingId"
            };
        }
    }

    @Override
    protected String getFileTypeDescription() {
        return "Emis problems file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getObservationGuid() {
        return super.getCell("ObservationGuid");
    }
    public CsvCell getParentProblemObservationGuid() {
        return super.getCell("ParentProblemObservationGuid");
    }
    public CsvCell getPatientGuid() {
        return super.getCell("PatientGuid");
    }
    public CsvCell getOrganisationGuid() {
        return super.getCell("OrganisationGuid");
    }
    public CsvCell getEndDate() {
        return super.getCell("EndDate");
    }
    public CsvCell getEndDatePrecision() {
        return super.getCell("EndDatePrecision");
    }
    public CsvCell getLastReviewDate() {
        return super.getCell("LastReviewDate");
    }
    public CsvCell getLastReviewDatePrecision() {
        return super.getCell("LastReviewDatePrecision");
    }
    public CsvCell getLastReviewUserInRoleGuid() {
        return super.getCell("LastReviewUserInRoleGuid");
    }
    public CsvCell getExpectedDuration() {
        return super.getCell("ExpectedDuration");
    }
    public CsvCell getSignificanceDescription() {
        return super.getCell("SignificanceDescription");
    }
    public CsvCell getProblemStatusDescription() {
        return super.getCell("ProblemStatusDescription");
    }
    public CsvCell getParentProblemRelationship() {
        return super.getCell("ParentProblemRelationship");
    }
    public CsvCell getDeleted() {
        return super.getCell("Deleted");
    }
    public CsvCell getComment() {
        return super.getCell("Comment");
    }
    public CsvCell getProcessingId() {
        return super.getCell("ProcessingId");
    }
    
    /*public String getObservationGuid() {
        return super.getString("ObservationGuid");
    }
    public String getParentProblemObservationGuid() {
        return super.getString("ParentProblemObservationGuid");
    }
    public String getPatientGuid() {
        return super.getString("PatientGuid");
    }
    public String getOrganisationGuid() {
        return super.getString("OrganisationGuid");
    }
    public Date getEndDate() throws TransformException {
        return super.getDate("EndDate");
    }
    public String getEndDatePrecision() {
        return super.getString("EndDatePrecision");
    }
    public Date getLastReviewDate() throws TransformException {
        return super.getDate("LastReviewDate");
    }
    public String getLastReviewDatePrecision() {
        return super.getString("LastReviewDatePrecision");
    }
    public String getLastReviewUserInRoleGuid() {
        return super.getString("LastReviewUserInRoleGuid");
    }
    public Integer getExpectedDuration() {
        return super.getInt("ExpectedDuration");
    }
    public String getSignificanceDescription() {
        return super.getString("SignificanceDescription");
    }
    public String getProblemStatusDescription() {
        return super.getString("ProblemStatusDescription");
    }
    public String getParentProblemRelationship() {
        return super.getString("ParentProblemRelationship");
    }
    public boolean getDeleted() {
        return super.getBoolean("Deleted");
    }
    public String getComment() {
        return super.getString("Comment");
    }
    public Integer getProcessingId() {
        return super.getInt("ProcessingId");
    }*/

}
