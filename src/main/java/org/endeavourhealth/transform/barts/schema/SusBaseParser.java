package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.transform.barts.AbstractFixedParser;
import org.endeavourhealth.transform.common.exceptions.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;

/*
    This class should only contain getters for fields which exist in all three file structures

 */
public class SusBaseParser extends AbstractFixedParser {
    private static final Logger LOG = LoggerFactory.getLogger(SusBaseParser.class);
    protected ArrayList<String> ICDSecondaryDiagnosisList = null;
    protected ArrayList<String> OPCSSecondaryProcedureCodeList = null;
    protected ArrayList<Date> OPCSSecondaryProcedureDateList = null;

    public SusBaseParser(String version, File f, boolean openParser, String dateFormat, String timeFormat) throws Exception {
        super(version, f, openParser, dateFormat, timeFormat);
    }

    public boolean nextRecord() throws Exception {
        OPCSSecondaryProcedureCodeList = null;
        OPCSSecondaryProcedureDateList = null;
        return super.nextRecord();
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
}
