package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.AbstractCharacterParser;
import org.endeavourhealth.transform.common.exceptions.FileFormatException;
import org.endeavourhealth.transform.common.exceptions.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class BulkProcedure extends AbstractCharacterParser {
    private static final Logger LOG = LoggerFactory.getLogger(BulkProcedure.class);

    public static final String DATE_FORMAT = "yyyy-mm-dd";
    public static final String TIME_FORMAT = "hh:mm:ss";
    public static final String DATE_TIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT;

    public BulkProcedure(String version, String filePath, boolean openParser) throws Exception {
        super(version, filePath, "\\|", openParser, DATE_FORMAT, TIME_FORMAT);

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
    public String getLocalPatientId() throws FileFormatException {
        return super.getString("MRN").trim();
    }
    public String getNHSNo() throws FileFormatException {
        return super.getString("NHSNo").replaceAll("\\-", "");
    }
    public String getConsultant() throws FileFormatException {
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

    public String getProcedureText() throws FileFormatException {
        return super.getString("ProcedureText").trim();
    }
    public String getComment() throws FileFormatException {
        return super.getString("Comment").trim();
    }
    public String getProcedureCode() throws FileFormatException {
        return super.getString("ProcedureCode").trim();
    }

    public Date getCreateDateTime() throws TransformException {
        return super.getDate("Create_DT_TM");
    }
    public Date getUpdateDateTime() throws TransformException {
        return super.getDateTime("Update_DT_TM");
    }
    public String getUpdatedBy() throws FileFormatException {
        return super.getString("UpdatedBy").trim();
    }
    public Long getEncounterId() throws FileFormatException {
        return Long.parseLong(super.getString("EncounterId").split("\\.")[0]);
    }
    public String getFINNo() throws FileFormatException {
        return super.getString("FINNo").trim();
    }

}