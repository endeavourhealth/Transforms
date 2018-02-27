package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractFixedParser;
import org.endeavourhealth.transform.common.FixedParserField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class Procedure extends AbstractFixedParser {
    private static final Logger LOG = LoggerFactory.getLogger(Procedure.class);

    public static final String DATE_FORMAT = "dd-MMM-yyyy";
    public static final String TIME_FORMAT = "hh:mm:ss";
    public static final String DATE_TIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT;

    public Procedure(String version, String filePath, boolean openParser) throws Exception {
        super(version, filePath, openParser, DATE_FORMAT, TIME_FORMAT);

        addFieldList(new FixedParserField("DOB",             1, 14));
        addFieldList(new FixedParserField("MRN",    13, 45));
        addFieldList(new FixedParserField("NHSNo",    59, 45));
        addFieldList(new FixedParserField("AdmissionDateTime",    105, 20));
        addFieldList(new FixedParserField("DischargeDateTime",    126, 20));
        addFieldList(new FixedParserField("Consultant",    285, 45));
        addFieldList(new FixedParserField("Procedure_DT_TM",    331, 20));
        addFieldList(new FixedParserField("ProcedureText",    352, 200));
        addFieldList(new FixedParserField("Comment",    553, 200));
        addFieldList(new FixedParserField("ProcedureCode",    754, 200));
        addFieldList(new FixedParserField("Create_DT_TM",          1139, 20));
        addFieldList(new FixedParserField("Update_DT_TM",          1160, 20));
        addFieldList(new FixedParserField("UpdatedBy",    1181, 45));
        addFieldList(new FixedParserField("EncounterId",          1227, 14));
        addFieldList(new FixedParserField("FINNo",          1242, 7));

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