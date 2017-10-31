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
        addFieldList(new FixedParserField("DischargeDate",    1112, 8));
        addFieldList(new FixedParserField("DischargeTime",    1120, 6));

        addFieldList(new FixedParserField("ConsultantCode",    1332, 8));

        addFieldList(new FixedParserField("ICDPrimaryDiagnosis",    1356, 6));
        addFieldList(new FixedParserField("ICDSecondaryDiagnosisList",    1363, 350));

        addFieldList(new FixedParserField("OPCSPrimaryProcedureCode",    1972, 4));
        addFieldList(new FixedParserField("OPCSPrimaryProcedureDate",    1976, 8));
        addFieldList(new FixedParserField("OPCSecondaryProcedureList",    2012, 2000));

        addFieldList(new FixedParserField("GP",    5325, 8));
        addFieldList(new FixedParserField("GPPractice",    5333, 12));

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

    public Date getDischargeDate() throws TransformException {
        return super.getDate("DischargeDate");
    }
    public Date getDischargeTime() throws TransformException {
        return super.getTime("DischargeTime");
    }
    public Date getDischargeDateTime() throws TransformException {
        return super.getDateTime("DischargeDate", "DischargeTime");
    }

    public String getConsultantCode() {
        return super.getString("ConsultantCode");
    }


}