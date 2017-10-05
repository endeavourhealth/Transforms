package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.AbstractFixedParser;
import org.endeavourhealth.transform.barts.FixedParserField;
import org.endeavourhealth.transform.common.exceptions.TransformException;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;

public class Sus extends AbstractFixedParser {
    public static final String DATE_FORMAT = "yyyyMMdd";
    public static final String TIME_FORMAT = "hhmmss";
    private ArrayList<String> ICDSecondaryDiagnosisList = null;
    private ArrayList<String> OPCSSecondaryProcedureList = null;

    public Sus(String version, File f, boolean openParser) throws Exception {
        super(version, f, openParser, DATE_FORMAT, TIME_FORMAT);

        addFieldList(new FixedParserField("CDSVersion",             1, 6));
        addFieldList(new FixedParserField("CDSRecordType",          7, 3));
        addFieldList(new FixedParserField("CDSReplacementgroup",    10, 3));
        addFieldList(new FixedParserField("CDSUniqueID",    16, 35));
        addFieldList(new FixedParserField("CDSUpdateType",    51, 1));
        addFieldList(new FixedParserField("MRN",    284, 10));
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

        addFieldList(new FixedParserField("AdmissionDate",    1052, 8));
        addFieldList(new FixedParserField("AdmissionTime",    1060, 6));
        addFieldList(new FixedParserField("ConsultantCode",    1332, 8));

        addFieldList(new FixedParserField("ICDPrimaryDiagnosis",    1356, 6));
        addFieldList(new FixedParserField("ICDSecondaryDiagnosisList",    1363, 350));

        addFieldList(new FixedParserField("OPCSPrimaryProcedureCode",    1972, 4));
        addFieldList(new FixedParserField("OPCSPrimaryProcedureDate",    1976, 8));
        addFieldList(new FixedParserField("OPCSecondaryProcedureList",    2052, 2000));

    }

    public int getCDSRecordType() {
        return super.getInt("CDSRecordType");
    }
    public String getCDSUniqueID() {
        return super.getString("CDSUniqueID");
    }
    // 1 = Delete, 9 = New/Replace
    public int getCDSUpdateType() {
        return super.getInt("CDSUpdateType");
    }

    public String getLocalPatientId() {
        return super.getString("MRN");
    }

    public String getDOB() {
        return super.getString("DOB");
    }
    public String getPatientTitle() {
        return super.getString("PatientTitle");
    }
    public String getPatientForename() {
        return super.getString("PatientForename");
    }
    public String getPatientSurname() {
        return super.getString("PatientSurname");
    }

    public String getAddressType() {
        return super.getString("AddressType");
    }
    public String getUnstructuredAddress() {
        return super.getString("UnstructuredAddress");
    }
    public String getAddress1() {
        return super.getString("Address1");
    }
    public String getAddress2() {
        return super.getString("Address2");
    }
    public String getAddress3() {
        return super.getString("Address3");
    }
    public String getAddress4() {
        return super.getString("Address4");
    }
    public String getAddress5() {
        return super.getString("Address5");
    }
    public String getPostCode() {
        return super.getString("PostCode");
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
        for (int i = 0; i < 49; i++) {
            int startPos = i * 7;
            String code = listString.substring(startPos, startPos + 6);
            if (code != null && code.length() > 0) {
                ICDSecondaryDiagnosisList.add(code.substring(0, 5));
            }
        }
    }

    public String getOPCSPrimaryProcedureCode() {
        return super.getString("OPCSPrimaryProcedureCode");
    }
    public Date getOPCSPrimaryProcedureDate() throws TransformException {
        return super.getDate("OPCSPrimaryProcedureDate");
    }

    public int getOPCSecondaryProcedureCodeCount() {
        // have the code string already been split ?
        if (OPCSSecondaryProcedureList == null) {
            splitOPCSSecondaryProcedureList();
        }
        return OPCSSecondaryProcedureList.size();
    }

    public String getOPCSecondaryProcedureCode(int pos) {
        // have the code string already been split ?
        if (OPCSSecondaryProcedureList == null) {
            splitOPCSSecondaryProcedureList();
        }
        if (OPCSSecondaryProcedureList.size() > 0) {
            return OPCSSecondaryProcedureList.get(pos);
        } else {
            return "";
        }
    }

    private void splitOPCSSecondaryProcedureList() {
        OPCSSecondaryProcedureList = new ArrayList<String> ();
        // Each code-set is 40 characters (6 for code + 1 for indicator) - only code is used
        String listString = super.getString("OPCSecondaryProcedureList");
        for (int i = 0; i < 49; i++) {
            int startPos = i * 40;
            String code = listString.substring(startPos, startPos + 6);
            if (code != null && code.length() > 0) {
                OPCSSecondaryProcedureList.add(code.substring(0, 3));
            }
        }
    }

}