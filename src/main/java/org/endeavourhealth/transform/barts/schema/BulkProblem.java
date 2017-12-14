package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.AbstractCharacterParser;
import org.endeavourhealth.transform.common.exceptions.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class BulkProblem extends AbstractCharacterParser {
    private static final Logger LOG = LoggerFactory.getLogger(BulkProblem.class);

    public static final String DATE_FORMAT = "yyyy-mm-dd";
    public static final String TIME_FORMAT = "hh:mm:ss";
    public static final String DATE_TIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT;

    public BulkProblem(String version, String filePath, boolean openParser) throws Exception {
        super(version, filePath, "\\|", openParser, DATE_FORMAT, TIME_FORMAT);

        addFieldList("ProblemId");
        addFieldList("Update_DT_TM");
        addFieldList("Person_Id");
        addFieldList("MRN");
        addFieldList("Problem");
        addFieldList("AnnotatedDisp");
        addFieldList("Qualifier");
        addFieldList("Confirmation");
        addFieldList("Classification");
        addFieldList("Onset_Precision");
        addFieldList("OnsetDate");
        addFieldList("StatusLifecycle");
        addFieldList("Lifecycle_Cancelled_Rsn");
        addFieldList("Severity_Class");
        addFieldList("Severity");
        addFieldList("Course");
        addFieldList("Persistence");
        addFieldList("Ranking");
        addFieldList("Certainty");
        addFieldList("Probability");
        addFieldList("Prognosis");
        addFieldList("Person_Aware");
        addFieldList("Family_Aware");
        addFieldList("Prognosis_Aware");
        addFieldList("Org_Name");
        addFieldList("ProblemCode");
        addFieldList("Vocabulary");
        addFieldList("Axis");
        addFieldList("Description");
        addFieldList("UpdatedBy");
        addFieldList("Source_System");
        addFieldList("Ctrl_Id");
        addFieldList("Record_Updated_Dt");
    }

    public Long getProblemId() {
        String ret = super.getString("ProblemId").split("\\.")[0];
        return Long.parseLong(ret);
    }

    public Date getUpdateDateTime() throws TransformException {
        return super.getDateTime("Update_DT_TM");
    }

    public String getLocalPatientId() {
        return super.getString("MRN").trim();
    }
    public String getProblem() {
        return super.getString("Problem").trim();
    }
    public String getAnnotatedDisp() {
        return super.getString("AnnotatedDisp").trim();
    }
    public String getConfirmation() {
        return super.getString("Confirmation");
    }

    public Date getOnsetDate() throws TransformException {
        return super.getDate("OnsetDate");
    }
    public String getOnsetDateAsString() throws TransformException {
        return super.getString("OnsetDate");
    }

    public String getStatusLifecycle() {
        return super.getString("StatusLifecycle");
    }
    public String getSeverity() {
        return super.getString("Severity").trim();
    }
    public String getProblemCode() {
        return super.getString("ProblemCode").trim();
    }
    public String getVocabulary() {
        return super.getString("Vocabulary").trim();
    }
    public String getDescription() {
        return super.getString("Description").trim();
    }
    public String getUpdatedBy() {
        return super.getString("UpdatedBy").trim();
    }

}