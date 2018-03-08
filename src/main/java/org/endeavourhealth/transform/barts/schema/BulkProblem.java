package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class BulkProblem extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(BulkProblem.class);

    public static final String DATE_FORMAT = "yyyy-mm-dd";
    public static final String TIME_FORMAT = "hh:mm:ss";
    public static final String DATE_TIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT;

    public BulkProblem(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, BartsCsvToFhirTransformer.CSV_FORMAT, DATE_FORMAT, TIME_FORMAT);
    }


    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[] {
                "ProblemId",
                "Update_DT_TM",
                "Person_Id",
                "MRN",
                "Problem",
                "AnnotatedDisp",
                "Qualifier",
                "Confirmation",
                "Classification",
                "Onset_Precision",
                "OnsetDate",
                "Status_Precision",
                "Status_Date",
                "StatusLifecycle",
                "Lifecycle_Cancelled_Rsn",
                "Severity_Class",
                "Severity",
                "Course",
                "Persistence",
                "Ranking",
                "Certainty",
                "Probability",
                "Prognosis",
                "Person_Aware",
                "Family_Aware",
                "Prognosis_Aware",
                "Org_Name",
                "ProblemCode",
                "Vocabulary",
                "Axis",
                "Description",
                "UpdatedBy",
                "Source_System",
                "Ctrl_Id",
                "Record_Updated_Dt",
        };
    }

    @Override
    protected String getFileTypeDescription() {
        return "Cerner Bulk 2.1 problems file";
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    public CsvCell getProblemId() {
        return super.getCell("ProblemId");
    }

    public CsvCell getUpdateDateTime() {
        return super.getCell("Update_DT_TM");
    }

    public CsvCell getLocalPatientId() {
        return super.getCell("MRN");
    }
    public CsvCell getProblem() {
        return super.getCell("Problem");
    }
    public CsvCell getAnnotatedDisp() {
        return super.getCell("AnnotatedDisp");
    }
    public CsvCell getConfirmation() {
        return super.getCell("Confirmation");
    }

    public CsvCell getOnsetDate() {
        return super.getCell("OnsetDate");
    }

    public CsvCell getStatusLifecycle() {
        return super.getCell("StatusLifecycle");
    }
    public CsvCell getSeverity() {
        return super.getCell("Severity");
    }
    public CsvCell getProblemCode() {
        return super.getCell("ProblemCode");
    }
    public CsvCell getVocabulary() {
        return super.getCell("Vocabulary");
    }
    public CsvCell getDescription() {
        return super.getCell("Description");
    }
    public CsvCell getUpdatedBy() {
        return super.getCell("UpdatedBy");
    }

    /*public Long getProblemId() throws FileFormatException {
        String ret = super.getString("ProblemId").split("\\.")[0];
        return Long.parseLong(ret);
    }

    public Date getUpdateDateTime() throws TransformException {
        return super.getDate("Update_DT_TM");
    }

    public String getLocalPatientId() throws FileFormatException {
        return super.getString("MRN").trim();
    }
    public String getProblem() throws FileFormatException {
        return super.getString("Problem").trim();
    }
    public String getAnnotatedDisp() throws FileFormatException {
        return super.getString("AnnotatedDisp").trim();
    }
    public String getConfirmation() throws FileFormatException {
        return super.getString("Confirmation");
    }

    public Date getOnsetDate() throws TransformException {
        return super.getDate("OnsetDate");
    }
    public String getOnsetDateAsString() throws TransformException {
        return super.getString("OnsetDate");
    }

    public String getStatusLifecycle() throws FileFormatException {
        return super.getString("StatusLifecycle");
    }
    public String getSeverity() throws FileFormatException {
        return super.getString("Severity").trim();
    }
    public String getProblemCode() throws FileFormatException {
        return super.getString("ProblemCode").trim();
    }
    public String getVocabulary() throws FileFormatException {
        return super.getString("Vocabulary").trim();
    }
    public String getDescription() throws FileFormatException {
        return super.getString("Description").trim();
    }
    public String getUpdatedBy() throws FileFormatException {
        return super.getString("UpdatedBy").trim();
    }*/

}