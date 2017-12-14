package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.AbstractCharacterParser;
import org.endeavourhealth.transform.barts.AbstractFixedParser;
import org.endeavourhealth.transform.barts.FixedParserField;
import org.endeavourhealth.transform.common.exceptions.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Date;

public class BulkProcedure extends AbstractCharacterParser {
    private static final Logger LOG = LoggerFactory.getLogger(BulkProcedure.class);

    public static final String DATE_FORMAT = "yyyy-mm-dd";
    public static final String TIME_FORMAT = "hh:mm:ss";
    public static final String DATE_TIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT;

    public BulkProcedure(String version, File f, boolean openParser) throws Exception {
        super(version, f, "\\|", openParser, DATE_FORMAT, TIME_FORMAT);

        addFieldList("DOB");
        addFieldList("MRN");
        addFieldList("NHSNo");
        addFieldList("AdmissionDateTime");
        addFieldList("DischargeDateTime");
        addFieldList("Trtmt_Func");
        addFieldList("Specialty");
        addFieldList("Ward");
        addFieldList("Consultant");
        addFieldList("Procedure_DT_TM");
        addFieldList("ProcedureText");
        addFieldList("Comment");
        addFieldList("ProcedureCode");
        addFieldList("Proc_Cd_Type");
        addFieldList("Error");
        addFieldList("Encounter_Type");
        addFieldList("Site");
        addFieldList("Create_DT_TM");
        addFieldList("Update_DT_TM");
        addFieldList("UpdatedBy");
        addFieldList("EncounterId");
        addFieldList("FINNo");
        addFieldList("Source_System");
        addFieldList("Ctrl_Id");
        addFieldList("Record_Updated_Dt");

    }

    public Date getDOB() throws TransformException {
        return super.getDate("DOB");
    }
    public String getLocalPatientId() {
        return super.getString("MRN").trim();
    }
    public String getNHSNo() {
        return super.getString("NHSNo").replaceAll("\\-", "");
    }
    public String getConsultant() {
        return super.getString("Consultant").trim();
    }

    public Date getProcedureDateTime() throws TransformException {
        return super.getDate("Procedure_DT_TM");
    }
    public String getProcedureDateTimeAsString() throws TransformException {
        return super.getString("Procedure_DT_TM");
    }

    public Date getAdmissionDateTime() throws TransformException {
        return super.getDateTime("AdmissionDateTime");
    }
    public String getAdmissionDateTimeAsString() throws TransformException {
        return super.getString("AdmissionDateTime");
    }

    public Date getDischargeDateTime() throws TransformException {
        return super.getDateTime("DischargeDateTime");
    }
    public String getDischargeDateTimeAsString() throws TransformException {
        return super.getString("DischargeDateTime");
    }

    public String getProcedureText() {
        return super.getString("ProcedureText").trim();
    }
    public String getComment() {
        return super.getString("Comment").trim();
    }
    public String getProcedureCode() {
        return super.getString("ProcedureCode").trim();
    }

    public Date getCreateDateTime() throws TransformException {
        return super.getDate("Create_DT_TM");
    }
    public Date getUpdateDateTime() throws TransformException {
        return super.getDateTime("Update_DT_TM");
    }
    public String getUpdatedBy() {
        return super.getString("UpdatedBy").trim();
    }
    public Long getEncounterId() {
        return Long.parseLong(super.getString("EncounterId").split("\\.")[0]);
    }
    public String getFINNo() {
        return super.getString("FINNo").trim();
    }

}