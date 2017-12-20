package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.AbstractCharacterParser;
import org.endeavourhealth.transform.common.exceptions.FileFormatException;
import org.endeavourhealth.transform.common.exceptions.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class BulkDiagnosis extends AbstractCharacterParser {
    private static final Logger LOG = LoggerFactory.getLogger(BulkDiagnosis.class);

    public static final String DATE_FORMAT = "yyyy-mm-dd";
    public static final String TIME_FORMAT = "hh:mm:ss";
    public static final String DATE_TIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT;

    public BulkDiagnosis(String version, String filePath, boolean openParser) throws Exception {
        super(version, filePath, "\\|", openParser, DATE_FORMAT, TIME_FORMAT);

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
        addFieldList("Source_System");
        addFieldList("Ctrl_Id");
        addFieldList("Record_Updated_Dt");

    }

    public Long getDiagnosisId() throws FileFormatException {
        return Long.parseLong(super.getString("DiagnosisId").trim().split("\\.")[0]);
    }

    public Date getUpdateDateTime() throws TransformException {
        return super.getDateTime("Update_DT_TM");
    }

    public String getActiveIndicator() throws FileFormatException {
        return super.getString("ActiveIndicator");
    }

    public boolean isActive() throws FileFormatException {
        int val = super.getInt("ActiveIndicator");
        if (val == 1) {
            return true;
        } else {
            return false;
        }
    }

    public Long getPersonId() throws FileFormatException {
        return Long.parseLong(super.getString("PersonId").trim().split("\\.")[0]);
    }

    public Long getEncounterId() throws FileFormatException {
        return Long.parseLong(super.getString("EncounterId").trim().split("\\.")[0]);
    }
    public String getLocalPatientId() throws FileFormatException {
        return super.getString("MRN").trim();
    }
    public String getFINNbr() throws FileFormatException {
        return super.getString("FINNbr").trim();
    }
    public String getDiagnosis() throws FileFormatException {
        return super.getString("Diagnosis").trim();
    }

    public Date getDiagnosisDate() throws TransformException {
        return super.getDate("DiagnosisDate");
    }
    public String getDiagnosisDateAsString() throws TransformException {
        return super.getString("DiagnosisDate");
    }

    public String getDiagnosisCode() throws FileFormatException {
        return super.getString("DiagnosisCode").trim();
    }
    public String getVocabulary() throws FileFormatException {
        return super.getString("Vocabulary").trim();
    }
    public String getSecondaryDescription() throws FileFormatException {
        return super.getString("SecondaryDescription").trim();
    }

}