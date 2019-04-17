package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractFixedParser;
import org.endeavourhealth.transform.common.CsvCell;
import org.endeavourhealth.transform.common.FixedParserField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

public class Procedure extends AbstractFixedParser {
    private static final Logger LOG = LoggerFactory.getLogger(Procedure.class);

    public static final String DATE_FORMAT = "dd-MMM-yyyy";
    public static final String TIME_FORMAT = "hh:mm:ss";

    public Procedure(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, DATE_FORMAT, TIME_FORMAT);
    }

    public CsvCell getProcedureCodeType() {
        return super.getCell("Proc_Cd_Type");
    }

    public CsvCell getProcedureCode() {
        return super.getCell("Proc_Cd");
    }

    public CsvCell getMrn() {
        return super.getCell("MRN");
    }

    public CsvCell getConsultant() {
        return super.getCell("Consultant");
    }

    public CsvCell getEncounterId() {
        return super.getCell("Encntr_Id");
    }

    public CsvCell getProcedureText() {
        return super.getCell("Proc_Txt");
    }

    public CsvCell getComment() {
        return super.getCell("Comment");
    }

    public CsvCell getProcedureDateTime() {
        return super.getCell("Proc_Dt_Tm");
    }

    public CsvCell getCreateDateTime() {
        return super.getCell("Create_Dt_Tm");
    }

    public CsvCell getUpdatedBy() {
        return super.getCell("Updt_By");
    }

    public CsvCell getWard() {return super.getCell("Ward");}

    public CsvCell getSite() {return super.getCell("Site");}

    public Date getDOB() throws TransformException {return super.getDate("DOB");}

    public String getNHSNo() {
        return super.getString("NHS_No").replaceAll("\\-", "");
    }


    public Date getAdmissionDateTime() throws TransformException {return super.getDateTime("Admit_Dt_Tm");}

    public String getAdmissionDateTimeAsString() throws TransformException {
        return super.getString("Admit_Dt_Tm");
    }

    public Date getDischargeDateTime() throws TransformException {
        return super.getDateTime("Disch_Dt_Tm");
    }

    public String getDischargeDateTimeAsString() throws TransformException {
        return super.getString("Disch_Dt_Tm");
    }

    public Date getUpdateDateTime() throws TransformException {
        return super.getDateTime("Updt_Dt_Tm");
    }

    public String getFINNo() {
        return super.getString("FIN_No");
    }

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    @Override
    protected boolean skipFirstRow() {
        return true;
    }

    @Override
    protected List<FixedParserField> getFieldList(String version) {

        List<FixedParserField> ret = new ArrayList<>();

        ret.add(new FixedParserField("DOB", 1, 11));
        ret.add(new FixedParserField("MRN", 13, 45));
        ret.add(new FixedParserField("NHS_No", 59, 45));
        ret.add(new FixedParserField("Admit_Dt_Tm", 105, 20));
        ret.add(new FixedParserField("Disch_Dt_Tm", 126, 20));
        ret.add(new FixedParserField("Trtmt_Func", 147, 45));
        ret.add(new FixedParserField("Specialty", 193, 45));
        ret.add(new FixedParserField("Ward", 239, 45));
        ret.add(new FixedParserField("Consultant", 285, 45));
        ret.add(new FixedParserField("Proc_Dt_Tm", 331, 20));
        ret.add(new FixedParserField("Proc_Txt", 352, 200));
        ret.add(new FixedParserField("Comment", 553, 200));
        ret.add(new FixedParserField("Proc_Cd", 754, 200));
        ret.add(new FixedParserField("Proc_Cd_Type", 955, 45));
        ret.add(new FixedParserField("Error", 1001, 45));
        ret.add(new FixedParserField("Enc_Type", 1047, 45));
        ret.add(new FixedParserField("Site", 1093, 45));
        ret.add(new FixedParserField("Create_Dt_Tm", 1139, 20));
        ret.add(new FixedParserField("Updt_Dt_Tm", 1160, 20));
        ret.add(new FixedParserField("Updt_By", 1181, 45));
        ret.add(new FixedParserField("Encntr_Id", 1227, 14));
        ret.add(new FixedParserField("FIN_No", 1242, 7));

        return ret;
    }

    /*@Override
    protected List<FixedParserField> getFieldList(String version) {

        List<FixedParserField> ret = new ArrayList<>();

        ret.add(new FixedParserField("DOB",             1, 14));
        ret.add(new FixedParserField("MRN",    13, 45));
        ret.add(new FixedParserField("NHSNo",    59, 45));
        ret.add(new FixedParserField("AdmissionDateTime",    105, 20));
        ret.add(new FixedParserField("DischargeDateTime",    126, 20));
        ret.add(new FixedParserField("Consultant",    285, 45));
        ret.add(new FixedParserField("Procedure_DT_TM",    331, 20));
        ret.add(new FixedParserField("ProcedureText",    352, 200));
        ret.add(new FixedParserField("Comment",    553, 200));
        ret.add(new FixedParserField("ProcedureCode",    754, 200));
        ret.add(new FixedParserField("Create_DT_TM",          1139, 20));
        ret.add(new FixedParserField("Update_DT_TM",          1160, 20));
        ret.add(new FixedParserField("UpdatedBy",    1181, 45));
        ret.add(new FixedParserField("EncounterId",          1227, 14));
        ret.add(new FixedParserField("FINNo",          1242, 7));
        
        return ret;
    }*/

}