package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.FixedParserField;
import org.endeavourhealth.transform.common.exceptions.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Date;

public class SusInpatient extends SusBaseParser {
    private static final Logger LOG = LoggerFactory.getLogger(SusInpatient.class);
    public static final String DATE_FORMAT = "yyyyMMdd";
    public static final String TIME_FORMAT = "hhmmss";

    public SusInpatient(String version, File f, boolean openParser) throws Exception {
        super(version, f, openParser, DATE_FORMAT, TIME_FORMAT);

        addFieldList(new FixedParserField("CDSVersion",             1, 6));
        addFieldList(new FixedParserField("CDSRecordType",          7, 3));
        addFieldList(new FixedParserField("CDSReplacementgroup",    10, 3));
        addFieldList(new FixedParserField("CDSUniqueID",    16, 35));
        addFieldList(new FixedParserField("CDSUpdateType",    51, 1));
        addFieldList(new FixedParserField("MRN",    284, 10));
        addFieldList(new FixedParserField("NHSNo",    308, 10));
        addFieldList(new FixedParserField("DOB",    321, 8));
        addFieldList(new FixedParserField("PatientTitle",    471, 35));
        addFieldList(new FixedParserField("PatientForename",    506, 35));
        addFieldList(new FixedParserField("PatientSurname",    541, 35));

        addFieldList(new FixedParserField("AddressType",    646, 2));
        addFieldList(new FixedParserField("UnstructuredAddress",    648, 175));
        addFieldList(new FixedParserField("Address1",    823, 35));
        addFieldList(new FixedParserField("Address2",    858, 35));
        addFieldList(new FixedParserField("Address3",    893, 35));
        addFieldList(new FixedParserField("Address4",    928, 35));
        addFieldList(new FixedParserField("Address5",    963, 35));
        addFieldList(new FixedParserField("PostCode",    998, 8));

        addFieldList(new FixedParserField("Gender",    1018, 1));
        addFieldList(new FixedParserField("EthnicCategory",    1021, 2));
        addFieldList(new FixedParserField("MaritalStatus",    1023, 1));

        addFieldList(new FixedParserField("AdmissionDate",    1052, 8));
        addFieldList(new FixedParserField("AdmissionTime",    1060, 6));
        addFieldList(new FixedParserField("ConsultantCode",    1332, 8));

        addFieldList(new FixedParserField("ICDPrimaryDiagnosis",    1356, 6));
        addFieldList(new FixedParserField("ICDSecondaryDiagnosisList",    1363, 350));

        addFieldList(new FixedParserField("OPCSPrimaryProcedureCode",    1972, 4));
        addFieldList(new FixedParserField("OPCSPrimaryProcedureDate",    1976, 8));
        addFieldList(new FixedParserField("OPCSecondaryProcedureList",    2012, 2000));

    }

    public Date getAdmissionDate() throws TransformException {
        return super.getDate("AdmissionDate");
    }
    public Date getAdmissionTime() throws TransformException {
        return super.getTime("AdmissionTime");
    }
    public Date getAdmissionDateTime() throws TransformException {
        return super.getDateTime("AdmissionDate", "AdmissionTime");
    }

    public String getConsultantCode() {
        return super.getString("ConsultantCode");
    }

    /*
    public String getICDPrimaryDiagnosis() {
        return super.getString("ICDPrimaryDiagnosis");
    }
    public String getICDSecondaryDiagnosis(int pos) {
        // have the code string already been split ?
        if (ICDSecondaryDiagnosisList == null) {
            splitICDSecondaryDiagnosisList();
        }
        if (ICDSecondaryDiagnosisList.size() > 0) {
            return ICDSecondaryDiagnosisList.get(pos);
        } else {
            return "";
        }
    }

    public int getICDSecondaryDiagnosisCount() {
        if (ICDSecondaryDiagnosisList == null) {
            splitICDSecondaryDiagnosisList();
        }
        return ICDSecondaryDiagnosisList.size();
    }

    private void splitICDSecondaryDiagnosisList() {
        ICDSecondaryDiagnosisList = new ArrayList<String> ();
        // Each code is 7 characters (6 for code + 1 for indicator) - only code is used
        String listString = super.getString("ICDSecondaryDiagnosisList");
        int startPos = 0;
        while (startPos + 6 < listString.length()) {
            String code = listString.substring(startPos, startPos + 6);
            if (code != null && code.length() > 0) {
                LOG.debug("Adding secondary diagnosis:" + code);
                ICDSecondaryDiagnosisList.add(code.substring(0, 5));
            }
            startPos = startPos + 6;
        }
    }

    public String getOPCSPrimaryProcedureCode() {
        return super.getString("OPCSPrimaryProcedureCode");
    }
    public Date getOPCSPrimaryProcedureDate() throws TransformException {
        return super.getDate("OPCSPrimaryProcedureDate");
    }

    public int getOPCSecondaryProcedureCodeCount() throws TransformException {
        // have the code string already been split ?
        if (OPCSSecondaryProcedureCodeList == null) {
            splitOPCSSecondaryProcedureCodeList();
        }
        return OPCSSecondaryProcedureCodeList.size();
    }

    public String getOPCSecondaryProcedureCode(int pos) throws TransformException {
        // have the code string already been split ?
        if (OPCSSecondaryProcedureCodeList == null) {
            splitOPCSSecondaryProcedureCodeList();
        }
        if (OPCSSecondaryProcedureCodeList.size() > 0) {
            return OPCSSecondaryProcedureCodeList.get(pos);
        } else {
            return "";
        }
    }

    public Date getOPCSecondaryProcedureDate(int pos) throws TransformException {
        // have the code string already been split ?
        if (OPCSSecondaryProcedureCodeList == null) {
            splitOPCSSecondaryProcedureCodeList();
        }
        if (OPCSSecondaryProcedureCodeList.size() > 0) {
            return OPCSSecondaryProcedureDateList.get(pos);
        } else {
            return null;
        }
    }

    private void splitOPCSSecondaryProcedureCodeList() throws TransformException {
        OPCSSecondaryProcedureCodeList = new ArrayList<String> ();
        OPCSSecondaryProcedureDateList = new ArrayList<Date> ();
        // Each code-set is 40 characters and consists of 6 fields (4 for code + 8 for date + 4 further sub-fields) - only code and date are used
        String listString = super.getString("OPCSecondaryProcedureList");
        int startPos = 0;
        while (startPos + 12 <= listString.length()) {
            String codeEntry = listString.substring(startPos, startPos + 4);
            String dateEntry = listString.substring(startPos + 4, startPos + 12);
            LOG.debug("Adding secondary procedure to list. StartPos=" + startPos +  " code=" + codeEntry + " date=" + dateEntry);
            OPCSSecondaryProcedureCodeList.add(codeEntry);
            OPCSSecondaryProcedureDateList.add(parseDate(dateEntry));
            startPos = startPos + 40;
        }
    }
    */
}