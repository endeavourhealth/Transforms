package org.endeavourhealth.transform.barts.schema;

import org.endeavourhealth.core.exceptions.TransformException;
import org.endeavourhealth.transform.common.FixedParserField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

public class SusInpatient extends SusBaseParser {
    private static final Logger LOG = LoggerFactory.getLogger(SusInpatient.class);
    public static final String DATE_FORMAT = "yyyyMMdd";
    public static final String TIME_FORMAT = "hhmmss";

    public SusInpatient(UUID serviceId, UUID systemId, UUID exchangeId, String version, String filePath) throws Exception {
        super(serviceId, systemId, exchangeId, version, filePath, DATE_FORMAT, TIME_FORMAT);
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

    @Override
    protected boolean isFileAudited() {
        return true;
    }

    @Override
    protected boolean skipFirstRow() {
        return false;
    }

    @Override
    protected List<FixedParserField> getFieldList(String version) {

        List<FixedParserField> ret = new ArrayList<>();

        ret.add(new FixedParserField("CDSVersion",             1, 6));
        ret.add(new FixedParserField("CDSRecordType",          7, 3));
        ret.add(new FixedParserField("CDSReplacementgroup",    10, 3));
        ret.add(new FixedParserField("CDSUniqueID",    16, 35));
        ret.add(new FixedParserField("CDSUpdateType",    51, 1));
        ret.add(new FixedParserField("MRN",    284, 10));
        ret.add(new FixedParserField("NHSNo",    308, 10));
        ret.add(new FixedParserField("DOB",    321, 8));
        ret.add(new FixedParserField("PatientTitle",    471, 35));
        ret.add(new FixedParserField("PatientForename",    506, 35));
        ret.add(new FixedParserField("PatientSurname",    541, 35));

        ret.add(new FixedParserField("AddressType",    646, 2));
        ret.add(new FixedParserField("UnstructuredAddress",    648, 175));
        ret.add(new FixedParserField("Address1",    823, 35));
        ret.add(new FixedParserField("Address2",    858, 35));
        ret.add(new FixedParserField("Address3",    893, 35));
        ret.add(new FixedParserField("Address4",    928, 35));
        ret.add(new FixedParserField("Address5",    963, 35));
        ret.add(new FixedParserField("PostCode",    998, 8));

        ret.add(new FixedParserField("Gender",    1018, 1));
        ret.add(new FixedParserField("EthnicCategory",    1021, 2));
        ret.add(new FixedParserField("MaritalStatus",    1023, 1));

        ret.add(new FixedParserField("AdmissionDate",    1052, 8));
        ret.add(new FixedParserField("AdmissionTime",    1060, 6));
        ret.add(new FixedParserField("DischargeDate",    1112, 8));
        ret.add(new FixedParserField("DischargeTime",    1120, 6));

        ret.add(new FixedParserField("ConsultantCode",    1332, 8));

        ret.add(new FixedParserField("ICDPrimaryDiagnosis",    1356, 6));
        ret.add(new FixedParserField("ICDSecondaryDiagnosisList",    1363, 350));

        ret.add(new FixedParserField("OPCSPrimaryProcedureCode",    1972, 4));
        ret.add(new FixedParserField("OPCSPrimaryProcedureDate",    1976, 8));
        ret.add(new FixedParserField("OPCSecondaryProcedureList",    2012, 2000));

        ret.add(new FixedParserField("GP",    5325, 8));
        ret.add(new FixedParserField("GPPractice",    5333, 12));
        
        return ret;
        
    }
}