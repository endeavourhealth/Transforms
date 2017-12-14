package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.AbstractCharacterParser;
import org.endeavourhealth.transform.barts.AbstractFixedParser;
import org.endeavourhealth.transform.barts.FixedParserField;
import org.endeavourhealth.transform.common.exceptions.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Date;

public class BulkDiagnosis extends AbstractCharacterParser {
    private static final Logger LOG = LoggerFactory.getLogger(BulkDiagnosis.class);

    public static final String DATE_FORMAT = "yyyy-mm-dd";
    public static final String TIME_FORMAT = "hh:mm:ss";
    public static final String DATE_TIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT;

    public BulkDiagnosis(String version, File f, boolean openParser) throws Exception {
        super(version, f, "\\|", openParser, DATE_FORMAT, TIME_FORMAT);

        addFieldList("DiagnosisId");
        addFieldList("Update_DT_TM");
        addFieldList("ActiveIndicator");
        addFieldList("PersonId");
        addFieldList("EncounterId");
        addFieldList("MRN");
        addFieldList("FINNbr");
        addFieldList("Diagnosis");
        addFieldList("Qualifier");
        addFieldList("Confirmation");
        addFieldList("DiagnosisDate");
        addFieldList("Classification");
        addFieldList("Clin_Service");
        addFieldList("Diag_Type");
        addFieldList("Rank");
        addFieldList("Diag_Prnsl");
        addFieldList("Severity_Class");
        addFieldList("Severity");
        addFieldList("Certainty");
        addFieldList("Probability");
        addFieldList("Org_Name");
        addFieldList("DiagnosisCode");
        addFieldList("Vocabulary");
        addFieldList("Axis");
        addFieldList("SecondaryDescription");
    }

    public Long getDiagnosisId() {
        return Long.parseLong(super.getString("DiagnosisId").trim().split("\\.")[0]);
    }

    public Date getUpdateDateTime() throws TransformException {
        return super.getDateTime("Update_DT_TM");
    }

    public String getActiveIndicator() {
        return super.getString("ActiveIndicator");
    }

    public boolean isActive() {
        int val = super.getInt("ActiveIndicator");
        if (val == 1) {
            return true;
        } else {
            return false;
        }
    }

    public Long getPersonId() {
        return Long.parseLong(super.getString("PersonId").trim().split("\\.")[0]);
    }

    public Long getEncounterId() {
        return Long.parseLong(super.getString("EncounterId").trim().split("\\.")[0]);
    }
    public String getLocalPatientId() {
        return super.getString("MRN").trim();
    }
    public String getFINNbr() {
        return super.getString("FINNbr").trim();
    }
    public String getDiagnosis() {
        return super.getString("Diagnosis").trim();
    }

    public Date getDiagnosisDate() throws TransformException {
        return super.getDate("DiagnosisDate");
    }
    public String getDiagnosisDateAsString() throws TransformException {
        return super.getString("DiagnosisDate");
    }

    public String getDiagnosisCode() {
        return super.getString("DiagnosisCode").trim();
    }
    public String getVocabulary() {
        return super.getString("Vocabulary").trim();
    }
    public String getSecondaryDescription() {
        return super.getString("SecondaryDescription").trim();
    }

}