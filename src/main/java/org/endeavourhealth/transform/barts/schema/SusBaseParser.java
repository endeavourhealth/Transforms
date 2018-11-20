package org.endeavourhealth.transform.barts.schema;

import com.google.common.base.Strings;
import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.AbstractFixedParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.UUID;

/*
    This class should only contain getters for fields which exist in all three file structures

 */
public abstract class SusBaseParser extends AbstractFixedParser {
    private static final Logger LOG = LoggerFactory.getLogger(SusBaseParser.class);
    protected ArrayList<String> ICDSecondaryDiagnosisList = null;
    protected ArrayList<String> OPCSSecondaryProcedureCodeList = null;
    protected ArrayList<Date> OPCSSecondaryProcedureDateList = null;
    protected ArrayList<String> OPCSSecondaryProcedureDateAsStringList = null;

    public SusBaseParser(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath, String dateFormat, String timeFormat) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, dateFormat, timeFormat);
    }

    public boolean nextRecord() throws Exception {
        ICDSecondaryDiagnosisList = null;
        OPCSSecondaryProcedureCodeList = null;
        OPCSSecondaryProcedureDateList = null;
        OPCSSecondaryProcedureDateAsStringList = null;
        return super.nextRecord();
    }

    public int getCDSRecordType() {
        return super.getInt("CDSRecordType");
    }

    public String getCDSUniqueID() {
        return super.getString("CDSUniqueIdentifier");
    }

    // 1 = Delete, 9 = New/Replace
    public int getCDSUpdateType() {
        return super.getInt("CDSUpdateType");
    }

    public String getLocalPatientId() {
        return super.getString("LocalPatientID");
    }

    public String getNHSNo() {
        return super.getString("NHSNumber");
    }

    public Date getDOB() throws TransformException {
        return super.getDate("PersonBirthDate");
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
        return super.getString("PatientAddressType");
    }

    public String getUnstructuredAddress() {
        return super.getString("PatientUnstructuredAddress");
    }

    public String getAddress1() {
        return super.getString("PatientAddressStructured1");
    }

    public String getAddress2() {
        return super.getString("PatientAddressStructured2");
    }

    public String getAddress3() {
        return super.getString("PatientAddressStructured3");
    }

    public String getAddress4() {
        return super.getString("PatientAddressStructured4");
    }

    public String getAddress5() {
        return super.getString("PatientAddressStructured5");
    }

    public String getPostCode() {
        return super.getString("Postcode");
    }

    public int getGender() {
        return Integer.parseInt(super.getString("PersonCurrentGender"));
    }

    public String getEthnicCategory() {
        return super.getString("EthnicCategory");
    }

    public String getGP() {
        return super.getString("GeneralMedicalPractitionerRegistered");
    }

    public String getGPPractice() {
        return super.getString("GPPracticeRegistered");
    }

    public String getICDPrimaryDiagnosis() {
        return super.getString("PrimaryDiagnosisICD");
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

    public String getICDSecondaryDiagnosis() {
        return super.getString("SecondaryDiagnosisICD");
    }

    public String getICDAdditionalSecondaryDiagnoses() {
        return super.getString("2nd50thSecondaryDiagnosisICD");
    }

    private void splitICDSecondaryDiagnosisList() {
        ICDSecondaryDiagnosisList = new ArrayList<>();

        //add secondary diagnosis
        String secondaryDiagnosis = getICDSecondaryDiagnosis();
        if (!Strings.isNullOrEmpty(secondaryDiagnosis)) {
            ICDSecondaryDiagnosisList.add(secondaryDiagnosis);
        }

        //add additional secondary ones
        // Each code is 7 characters (6 for code + 1 for indicator) - only code is used
        String listString = getICDAdditionalSecondaryDiagnoses();
        //LOG.trace("Diagnosis counter start string=" + listString);
        while (listString != null && listString.length() > 0) {
            String code = listString.substring(0, (listString.length() >= 6 ? 6 : listString.length())).trim();
            //LOG.trace("Diagnosis counter found code=" + code + "=");
            ICDSecondaryDiagnosisList.add(code);
            listString = listString.substring((listString.length() >= 7 ? 7 : listString.length()));
        }
    }

    /*public String getICDSecondaryDiagnosisList() {
        return super.getString("ICDSecondaryDiagnosisList");
    }

    private void splitICDSecondaryDiagnosisList() {
        ICDSecondaryDiagnosisList = new ArrayList<String> ();
        // Each code is 7 characters (6 for code + 1 for indicator) - only code is used
        String listString = getICDSecondaryDiagnosisList();
        //LOG.trace("Diagnosis counter start string=" + listString);
        while (listString != null && listString.length() > 0) {
            String code = listString.substring(0, (listString.length() >= 6 ? 6 : listString.length())).trim();
            //LOG.trace("Diagnosis counter found code=" + code + "=");
            ICDSecondaryDiagnosisList.add(code);
            listString = listString.substring((listString.length() >= 7 ? 7 : listString.length()));
        }
    }*/

    public String getOPCSPrimaryProcedureCode() {
        return super.getString("PrimaryProcedureOPCS");
    }

    public Date getOPCSPrimaryProcedureDate() throws TransformException {
        return super.getDate("PrimaryProcedureDate");
    }

    public String getOPCSPrimaryProcedureDateAsString() throws TransformException {
        return super.getString("PrimaryProcedureDate");
    }

    public String getOPCSecondaryProcedure() {
        return super.getString("SecondaryProcedureOPCS");
    }

    public String getOPCSecondaryProcedureDate() {
        return super.getString("SecondaryProcedureDate");
    }

    public String getAdditionalOPCSecondaryProcedures() {
        return super.getString("2nd50thSecondaryProceduresOPCS");
    }

    private void splitOPCSSecondaryProcedureCodeList() throws TransformException {
        OPCSSecondaryProcedureCodeList = new ArrayList<String>();
        OPCSSecondaryProcedureDateList = new ArrayList<Date>();
        OPCSSecondaryProcedureDateAsStringList = new ArrayList<String>();

        String secondaryProcedure = getOPCSecondaryProcedure();
        String secondaryProcedureDate = getOPCSecondaryProcedureDate();
        if (!Strings.isNullOrEmpty(secondaryProcedure)) {
            OPCSSecondaryProcedureCodeList.add(secondaryProcedure);
            OPCSSecondaryProcedureDateAsStringList.add(secondaryProcedureDate);
            OPCSSecondaryProcedureDateList.add(parseDate(secondaryProcedureDate));
        }

        // Each code-set is 40 characters and consists of 6 fields (4 for code + 8 for date + 4 further sub-fields) - only code and date are used
        String listString = getAdditionalOPCSecondaryProcedures();
        int startPos = 0;
        while (startPos + 12 <= listString.length()) {
            String codeEntry = listString.substring(startPos, startPos + 4);
            String dateEntry = listString.substring(startPos + 4, startPos + 12);
            //LOG.debug("Adding secondary procedure to list. StartPos=" + startPos +  " code=" + codeEntry + " date=" + dateEntry);
            OPCSSecondaryProcedureCodeList.add(codeEntry);
            OPCSSecondaryProcedureDateAsStringList.add(dateEntry);
            OPCSSecondaryProcedureDateList.add(parseDate(dateEntry));
            startPos = startPos + 40;
        }
    }

    /*public String getOPCSecondaryProcedureList() {
        return super.getString("OPCSecondaryProcedureList");
    }

    private void splitOPCSSecondaryProcedureCodeList() throws TransformException {
        OPCSSecondaryProcedureCodeList = new ArrayList<String> ();
        OPCSSecondaryProcedureDateList = new ArrayList<Date> ();
        OPCSSecondaryProcedureDateAsStringList = new ArrayList<String> ();
        // Each code-set is 40 characters and consists of 6 fields (4 for code + 8 for date + 4 further sub-fields) - only code and date are used
        String listString = getOPCSecondaryProcedureList();
        int startPos = 0;
        while (startPos + 12 <= listString.length()) {
            String codeEntry = listString.substring(startPos, startPos + 4);
            String dateEntry = listString.substring(startPos + 4, startPos + 12);
            //LOG.debug("Adding secondary procedure to list. StartPos=" + startPos +  " code=" + codeEntry + " date=" + dateEntry);
            OPCSSecondaryProcedureCodeList.add(codeEntry);
            OPCSSecondaryProcedureDateAsStringList.add(dateEntry);
            OPCSSecondaryProcedureDateList.add(parseDate(dateEntry));
            startPos = startPos + 40;
        }
    }*/

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

    public String getOPCSecondaryProcedureDateAsString(int pos) throws TransformException {
        // have the code string already been split ?
        if (OPCSSecondaryProcedureCodeList == null) {
            splitOPCSSecondaryProcedureCodeList();
        }
        if (OPCSSecondaryProcedureCodeList.size() > 0) {
            return OPCSSecondaryProcedureDateAsStringList.get(pos);
        } else {
            return null;
        }
    }


}
