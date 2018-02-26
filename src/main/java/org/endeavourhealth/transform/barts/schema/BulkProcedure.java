package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.barts.BartsCsvToFhirTransformer;
import org.endeavourhealth.transform.common.AbstractCsvParser;
import org.endeavourhealth.transform.common.exceptions.FileFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.UUID;

public class BulkProcedure extends AbstractCsvParser {
    private static final Logger LOG = LoggerFactory.getLogger(BulkProcedure.class);

    public static final String DATE_FORMAT = "yyyy-mm-dd";
    public static final String TIME_FORMAT = "hh:mm:ss";
    public static final String DATE_TIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT;

    public BulkProcedure(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, BartsCsvToFhirTransformer.CSV_FORMAT, DATE_FORMAT, TIME_FORMAT);
    }


    @Override
    protected String[] getCsvHeaders(String version) {
        return new String[] {
                "DOB",
                "MRN",
                "NHSNo",
                "AdmissionDateTime",
                "DischargeDateTime",
                "Trtmt_Func",
                "Specialty",
                "Ward",
                "Consultant",
                "Procedure_DT_TM",
                "ProcedureText",
                "Comment",
                "ProcedureCode",
                "Proc_Cd_Type",
                "Error",
                "Encounter_Type",
                "Site",
                "Create_DT_TM",
                "Update_DT_TM",
                "UpdatedBy",
                "EncounterId",
                "FINNo",
                "Source_System",
                "Ctrl_Id",
                "Record_Updated_Dt",
        };
    }

    @Override
    protected String getFileTypeDescription() {
        return "Cerner 2.1 Bulk Procedures file";
    }

    @Override
    protected boolean isFileAudited() {
        return false;
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
        return super.getDate("AdmissionDateTime");
    }
    public String getAdmissionDateTimeAsString() throws TransformException {
        return super.getString("AdmissionDateTime");
    }

    public Date getDischargeDateTime() throws TransformException {
        return super.getDate("DischargeDateTime");
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
        return super.getDate("Update_DT_TM");
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